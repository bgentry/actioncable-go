package actioncable

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jpillora/backoff"
)

// HeaderFunc is a function that returns HTTP headers or an error.
type HeaderFunc func() (*http.Header, error)

// Client is an ActionCable websocket client.
type Client struct {
	u                      string
	dialer                 *websocket.Dialer
	inactivityTimeout      time.Duration
	inactivityTimeoutTimer *time.Timer

	outboundc chan *Command
	subc      chan string

	// TODO: what actually needs a mutex??
	mu            sync.Mutex
	connHdrFunc   HeaderFunc
	subscriptions map[string]chan *EventOrErr
	closed        bool
	donec         chan struct{}
	waitc         chan struct{}
	ref           int
}

// NewClient creates a new Client that connects to the provided url using the
// HTTP headers from connHdrFunc, which will be called once for each connection
// attempt. If connHdrFunc returns an error when called, that will cause the
// connection attempt to fail.
func NewClient(url string, connHdrFunc HeaderFunc) *Client {
	c := &Client{
		u: url,
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		},
		inactivityTimeout: 6 * time.Second, // 2 * the 3 sec ping interval
		outboundc:         make(chan *Command, 32),
		subc:              make(chan string, 32),
		connHdrFunc:       connHdrFunc,
		subscriptions:     make(map[string]chan *EventOrErr),
		donec:             make(chan struct{}),
		waitc:             make(chan struct{}),
	}
	go c.connLoop()
	return c
}

// Close closes any active connection on the Client and stops it from making
// additional connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.donec != nil && !c.closed {
		close(c.donec)
		<-c.waitc
		c.closed = true
	}
}

// ErrAlreadySubscribed is returned when the client has already subscribed to
// a given channel.
var ErrAlreadySubscribed = errors.New("channel already subscribed")

// Subscribe establishes a subscription to a specific channel. If the channel
// is already subscribed, ErrAlreadySubscribed is returned. Otherwise an
// subscription channel will return. If the subscription is rejected at any
// point (including after a future reconnect), the channel will be closed. It
// will also be closed if Unsubscribe is called for that subscription.
//
// The channel will receive a confirm_subscription event anytime the
// subscription is confirmed (including every time the client reconnects) and
// will receive any events sent to that channel. Events may be missed during
// disconnects, so a confirm_subscription event might be a good time to
// resynchronize state that might be stale.
func (c *Client) Subscribe(channel string) (<-chan *EventOrErr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, alreadySubscribed := c.subscriptions[channel]; alreadySubscribed {
		return nil, ErrAlreadySubscribed
	}

	c.subscriptions[channel] = make(chan *EventOrErr, 32)

	// Add subscription command to outbound queue.
	go func() {
		c.subc <- channel
	}()

	return c.subscriptions[channel], nil
}

// Unsubscribe stops subscribing to channel.
func (c *Client) Unsubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, ok := c.subscriptions[channel]
	if !ok {
		return
	}
	delete(c.subscriptions, channel)
	close(ch)

	cmd := &Command{
		Command: "unsubscribe",
		Identifier: CommandIdentifier{
			Channel: channel,
		},
	}

	go func() {
		c.outboundc <- cmd
	}()
}

// Send an action on a channel. The "action" field of data will be overridden.
// The return channel will send an error if one is encountered, or will close
// if the send completes.
func (c *Client) Send(channel, action string, data map[string]interface{}) <-chan error {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["action"] = action
	errc := make(chan error, 1)
	b, err := json.Marshal(data)
	if err != nil {
		errc <- err
		return errc
	}
	doubleEncoded, err := json.Marshal(string(b))
	if err != nil {
		errc <- err
		return errc
	}
	cmd := &Command{
		Command: "message",
		Identifier: CommandIdentifier{
			Channel: channel,
		},
		Data: doubleEncoded,
		errc: errc,
	}
	c.outboundc <- cmd
	return errc
}

func (c *Client) connLoop() {
	b := backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}
	for {
		err := c.connOnce(c.u, b.Reset)
		if err != nil {
			log.Printf("conn error: %s", err)
		}
		log.Println("disconnected")
		select {
		case <-c.donec:
			close(c.waitc)
			return
			// TODO: backoff
		case <-time.After(b.Duration()):
			log.Println("reconnecting")
		}
	}
}

func (c *Client) drainSubc() {
	for {
		select {
		case <-c.subc:
			// discard
		default:
			return
		}
	}
}

func (c *Client) connOnce(url string, f func()) error {
	// per docs, this resp.Body doesn't need to be closed
	connHdr, err := c.connHdrFunc()
	if err != nil {
		return err
	}
	conn, _, err := c.dialer.Dial(c.u, *connHdr)
	if err != nil {
		return err
	}
	defer conn.Close()

	c.inactivityTimeoutTimer = time.NewTimer(c.inactivityTimeout)
	defer c.inactivityTimeoutTimer.Stop()

	recvc := make(chan EventOrErr, 1)

	go c.receiveMsg(conn, recvc)
	if err = checkWelcome(recvc); err != nil {
		return err
	}

	fmt.Printf("connected to %s\n", conn.RemoteAddr())
	if f != nil {
		f()
	}

	// drain the old subscription requests and resubscribe
	c.drainSubc()
	c.resubscribe()

	for {
		go c.receiveMsg(conn, recvc)

		select {
		case <-c.donec:
			return nil
		case eventOrErr := <-recvc:
			if eventOrErr.Err != nil {
				return err
			}
			c.handleEvent(eventOrErr.Event)
		case chanName := <-c.subc:
			cmd := &Command{
				Command: "subscribe",
				Identifier: CommandIdentifier{
					Channel: chanName,
				},
			}
			if err := conn.WriteJSON(cmd); err != nil {
				return err
			}
		case cmd := <-c.outboundc:
			if err := conn.WriteJSON(cmd); err != nil {
				// TODO: save cmd to a var to be written after reconnect
				if cmd.errc != nil {
					cmd.errc <- err
				}
				return err
			}
			if cmd.errc != nil {
				close(cmd.errc)
			}
		case <-c.inactivityTimeoutTimer.C:
			return fmt.Errorf("timeout waiting for ping from server")
		}
	}
}

func checkWelcome(recvc <-chan EventOrErr) error {
	eventOrErr := <-recvc
	if eventOrErr.Err != nil {
		return eventOrErr.Err
	}
	if eventOrErr.Event.Type != "welcome" {
		return fmt.Errorf("received unexpected %q message after connect", eventOrErr.Event.Type)
	}
	return nil
}

func (c *Client) handleEvent(evt *Event) {
	// If we've received any kind of event, the channel must be alive.
	c.inactivityTimeoutTimer.Reset(c.inactivityTimeout)
	switch evt.Type {
	case "ping":
		// do nothing
	case "reject_subscription":
		ch := c.getAndRemoveSub(evt.Identifier.Channel)
		if ch == nil {
			return
		}
		fmt.Printf("sub rejected: %#v %#v\n", evt, evt.Identifier)
		ch <- &EventOrErr{Err: errors.New("subscription rejected")}
		close(ch)
	default:
		ch := c.getSub(evt.Identifier.Channel)
		if ch == nil {
			log.Printf("received msg for unsubscribed channel: %s", evt.Identifier.Channel)
			return
		}
		select {
		case ch <- &EventOrErr{Event: evt}:
		default:
			log.Printf("no receiver ready, dropping message: %#v\n", evt)
		}
	}
}

func (c *Client) getAndRemoveSub(name string) chan *EventOrErr {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, ok := c.subscriptions[name]
	if !ok {
		return nil
	}
	delete(c.subscriptions, name)
	return ch
}

func (c *Client) getSub(name string) chan *EventOrErr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscriptions[name]
}

func (c *Client) receiveMsg(conn *websocket.Conn, recvc chan<- EventOrErr) {
	event := &Event{}
	if err := conn.ReadJSON(event); err != nil {
		recvc <- EventOrErr{Err: err}
		return
	}
	recvc <- EventOrErr{Event: event}
}

func (c *Client) resubscribe() {
	c.mu.Lock()
	defer c.mu.Unlock()

	chans := make([]string, 0, len(c.subscriptions))
	for name := range c.subscriptions {
		chans = append(chans, name)
	}

	go func() {
		for _, name := range chans {
			c.subc <- name
		}
	}()
}

// EventOrErr is an Event or error returned on a subscription channel.
type EventOrErr struct {
	Event *Event
	Err   error
}

// Command is a command issued on a channel.
type Command struct {
	Command    string            `json:"command"`
	Data       json.RawMessage   `json:"data,omitempty"`
	Identifier CommandIdentifier `json:"identifier"`
	errc       chan error
}

// CommandIdentifier identifies which Channel a Command occurs on.
type CommandIdentifier struct {
	// Channel is the name of the channel.
	Channel string
}

type innerIdentifier struct {
	Channel string `json:"channel"`
}

// MarshalJSON encodes the CommandIdentifier to JSON.
func (c *CommandIdentifier) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(innerIdentifier{
		Channel: c.Channel,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(b))
}

// UnmarshalJSON decodes the CommandIdentifier from JSON.
func (c *CommandIdentifier) UnmarshalJSON(data []byte) error {
	str := ""
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	inner := innerIdentifier{}
	if err := json.Unmarshal([]byte(str), &inner); err != nil {
		return err
	}
	c.Channel = inner.Channel
	return nil
}

// Event is a subscription event received on a channel.
type Event struct {
	Type string `json:"type"`

	Message    json.RawMessage    `json:"message"`
	Data       json.RawMessage    `json:"data"`
	Identifier *CommandIdentifier `json:"identifier"`
}
