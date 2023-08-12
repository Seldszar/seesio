package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/tidwall/gjson"
	"github.com/uptrace/bunrouter"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/maps"
	"golang.org/x/oauth2/clientcredentials"
)

type H = bunrouter.H

type Cheer struct {
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Bits      int64  `json:"amount"`
}

type Raid struct {
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Viewers   int64  `json:"count"`
}

type Subscription struct {
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Tier      string `json:"tier"`
	Months    int64  `json:"months"`
}

type SubscriptionGift struct {
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Total     int64  `json:"total"`
}

type Session struct {
	Cheers            map[string]*Cheer
	Raids             map[string]*Raid
	Subscriptions     map[string]*Subscription
	SubscriptionGifts map[string]*SubscriptionGift
}

func (s *Session) AddCheer(data *Cheer) {
	if v, ok := s.Cheers[data.UserName]; ok {
		data.Bits += v.Bits
	}

	s.Cheers[data.UserName] = data
}

func (s *Session) AddRaid(data *Raid) {
	if v, ok := s.Raids[data.UserName]; ok && data.Viewers > v.Viewers {
		data.Viewers = v.Viewers
	}

	s.Raids[data.UserName] = data
}

func (s *Session) AddSubscription(data *Subscription) {
	if v, ok := s.Subscriptions[data.UserName]; ok && data.Months > v.Months {
		data.Months = v.Months
	}

	s.Subscriptions[data.UserName] = data
}

func (s *Session) AddSubscriptionGift(data *SubscriptionGift) {
	if v, ok := s.SubscriptionGifts[data.UserName]; ok {
		data.Total += v.Total
	}

	s.SubscriptionGifts[data.UserName] = data
}

func (s Session) MarshalJSON() ([]byte, error) {
	res := H{
		"cheers":             maps.Values(s.Cheers),
		"raids":              maps.Values(s.Raids),
		"subscription_gifts": maps.Values(s.SubscriptionGifts),
		"subscriptions":      maps.Values(s.Subscriptions),
	}

	return json.Marshal(res)
}

func (s *Session) Reset() {
	clear(s.Cheers)
	clear(s.Raids)
	clear(s.Subscriptions)
	clear(s.SubscriptionGifts)
}

func reply(w http.ResponseWriter, statusCode int, value string) error {
	w.WriteHeader(statusCode)

	if value == "" {
		return nil
	}

	if _, err := io.WriteString(w, value); err != nil {
		return err
	}

	return nil
}

func subscribe(ctx *cli.Context, client *http.Client, eventType, eventVersion string, eventCondition H) ([]byte, error) {
	data := H{
		"type":      eventType,
		"version":   eventVersion,
		"condition": eventCondition,
		"transport": H{
			"method":   "webhook",
			"callback": fmt.Sprintf("%s/eventsub", ctx.String("api-base-url")),
			"secret":   ctx.String("api-secret-key"),
		},
	}

	buf, err := json.Marshal(data)

	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", "https://api.twitch.tv/helix/eventsub/subscriptions", bytes.NewBuffer(buf))

	if err != nil {
		return nil, err
	}

	req.Header.Set("Client-ID", ctx.String("twitch-client-id"))
	req.Header.Set("Client-Secret", ctx.String("twitch-client-secret"))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		return nil, err
	}

	return io.ReadAll(resp.Body)
}

func subscribeEvents(ctx *cli.Context, client *http.Client) error {
	userId := ctx.String("twitch-client-id")

	if _, err := subscribe(ctx, client, "channel.cheer", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.raid", "1", H{"to_broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.subscribe", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.subscribe.gift", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.subscribe.message ", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "stream.offline", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "stream.online", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	return nil
}

func verifyMessage(ctx *cli.Context, header http.Header, messageBody []byte) bool {
	parts := strings.SplitN(header.Get("Twitch-Eventsub-Message-Signature"), "=", 2)

	messageId := []byte(header.Get("Twitch-Eventsub-Message-Id"))
	messageTimestamp := []byte(header.Get("Twitch-Eventsub-Message-Timestamp"))

	bytes, err := hex.DecodeString(parts[1])

	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(ctx.String("api-secret-key")))

	mac.Write(messageId)
	mac.Write(messageTimestamp)
	mac.Write(messageBody)

	return hmac.Equal(bytes, mac.Sum(nil))
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out: os.Stdout,
	})

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "api-base-url",
				EnvVars:  []string{"API_BASE_URL"},
				Usage:    "Base URL used for receiving events",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "api-secret-key",
				EnvVars:  []string{"API_SECRET_KEY"},
				Usage:    "Secret key used for subscribing to events",
				Required: true,
			},
			&cli.IntFlag{
				Name:    "session-reset-delay",
				EnvVars: []string{"SESSION_RESET_DELAY"},
				Usage:   "Duration in minutes until the session will reset after the stream goes offline",
				Value:   5,
			},
			&cli.StringFlag{
				Name:     "twitch-client-id",
				EnvVars:  []string{"TWITCH_CLIENT_ID"},
				Usage:    "Twitch client ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "twitch-client-secret",
				EnvVars:  []string{"TWITCH_CLIENT_SECRET"},
				Usage:    "Twitch client secret",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "twitch-user-id",
				EnvVars:  []string{"TWITCH_USER_ID"},
				Usage:    "User ID used for listening to events",
				Required: true,
			},
		},
		Action: func(ctx *cli.Context) error {
			conf := &clientcredentials.Config{
				ClientID:     ctx.String("twitch-client-id"),
				ClientSecret: ctx.String("twitch-client-secret"),

				TokenURL: "https://id.twitch.tv/oauth2/token",
			}

			client := conf.Client(context.Background())
			router := bunrouter.New()

			var offlineTime time.Time

			session := Session{
				Cheers:            make(map[string]*Cheer),
				Raids:             make(map[string]*Raid),
				Subscriptions:     make(map[string]*Subscription),
				SubscriptionGifts: make(map[string]*SubscriptionGift),
			}

			knownMessageIds := make(map[string]struct{})

			router.GET("/", func(w http.ResponseWriter, req bunrouter.Request) error {
				return bunrouter.JSON(w, &H{
					"data": session,
				})
			})

			router.GET("/authorize/callback", func(w http.ResponseWriter, req bunrouter.Request) error {
				return reply(w, http.StatusOK, "You are now authorized, you can close this window.")
			})

			router.POST("/eventsub", func(w http.ResponseWriter, req bunrouter.Request) error {
				body, err := io.ReadAll(req.Body)

				if err != nil {
					return err
				}

				if verifyMessage(ctx, req.Header, body) {
					res := gjson.ParseBytes(body)

					messageId := res.Get("Twitch-Eventsub-Message-Id").String()

					if _, ok := knownMessageIds[messageId]; ok {
						return reply(w, http.StatusNoContent, "")
					}

					knownMessageIds[messageId] = struct{}{}

					switch req.Header.Get("Twitch-Eventsub-Message-Type") {
					case "notification":
						slog.Debug("Notification received", res.String())

						switch res.Get("subscription.type").String() {
						case "channel.cheer":
							data := &Cheer{
								UserLogin: res.Get("event.user_login").String(),
								UserName:  res.Get("event.user_name").String(),
								Bits:      res.Get("event.bits").Int(),
							}

							session.AddCheer(data)

						case "channel.raid":
							data := &Raid{
								UserLogin: res.Get("event.from_broadcaster_user_login").String(),
								UserName:  res.Get("event.from_broadcaster_user_name").String(),
								Viewers:   res.Get("event.viewers").Int(),
							}

							session.AddRaid(data)

						case "channel.subscribe", "channel.subscribe.message":
							if res.Get("event.is_gift").Bool() {
								break
							}

							data := &Subscription{
								UserLogin: res.Get("event.user_login").String(),
								UserName:  res.Get("event.user_name").String(),
								Tier:      res.Get("event.tier").String(),
								Months:    1,
							}

							if months := res.Get("event.cumulative_months").Int(); months > 0 {
								data.Months = months
							}

							session.AddSubscription(data)

						case "channel.subscribe.gift":
							data := &SubscriptionGift{
								UserLogin: res.Get("event.user_login").String(),
								UserName:  res.Get("event.user_name").String(),
								Total:     res.Get("event.total").Int(),
							}

							session.AddSubscriptionGift(data)

						case "stream.offline":
							offlineTime = time.Now()

						case "stream.online":
							if offlineTime.Add(time.Duration(ctx.Int("session-reset-delay")) * time.Minute).After(time.Now()) {
								break
							}

							session.Reset()
						}

					case "webhook_callback_verification":
						return reply(w, http.StatusOK, res.Get("challenge").String())
					}

					return reply(w, http.StatusNoContent, "")
				}

				return reply(w, http.StatusForbidden, "")
			})

			if err := subscribeEvents(ctx, client); err != nil {
				slog.Error("An error occured while subscribing to events, please authorize the application first: https://id.twitch.tv/oauth2/authorize?response_type=code&client_id=%s&redirect_uri=%s/authorize/callback&scope=bits:read+channel:read:subscriptions&force_verify=true", ctx.String("twitch-client-id"), ctx.String("api-base-url"))
			}

			return http.ListenAndServe(":3000", router)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err)
	}
}
