package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/uptrace/bunrouter"
	"github.com/urfave/cli/v2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/oauth2/twitch"
)

type H = bunrouter.H

type Cheer struct {
	UserID    string `json:"user_id"`
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Bits      int64  `json:"bits"`
}

type Follow struct {
	UserID     string    `json:"user_id"`
	UserLogin  string    `json:"user_login"`
	UserName   string    `json:"user_name"`
	FollowedAt time.Time `json:"followed_at"`
}

type Raid struct {
	UserID    string `json:"user_id"`
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Viewers   int64  `json:"viewers"`
}

type Subscription struct {
	UserID    string `json:"user_id"`
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Tier      string `json:"tier"`
	Months    int64  `json:"months"`
}

type SubscriptionGift struct {
	UserID    string `json:"user_id"`
	UserLogin string `json:"user_login"`
	UserName  string `json:"user_name"`
	Tier      string `json:"tier"`
	Total     int64  `json:"total"`
}

func decodeValue[T any](data string) (v T, err error) {
	return v, json.Unmarshal([]byte(data), &v)
}

func encodeValue[T any](v T) (string, error) {
	data, err := json.Marshal(v)

	if err != nil {
		return "", err
	}

	return string(data), nil
}

func ascend[T any](tx *buntdb.Tx, index string) ([]T, error) {
	items := make([]T, 0)

	err := tx.Ascend(index, func(key, value string) bool {
		if item, err := decodeValue[T](value); err == nil {
			items = append(items, item)
		}

		return true
	})

	if err != nil {
		return nil, err
	}

	return items, nil
}

func get[T any](tx *buntdb.Tx, key string) (*T, error) {
	value, err := tx.Get(key)

	if err != nil {
		return nil, err
	}

	return decodeValue[*T](value)
}

func set[T any](tx *buntdb.Tx, key string, item T) error {
	data, err := encodeValue(&item)

	if err != nil {
		return err
	}

	if _, _, err := tx.Set(key, data, nil); err != nil {
		return err
	}

	return nil
}

func addCheer(tx *buntdb.Tx, data *Cheer) error {
	key := fmt.Sprintf("cheer:%s", data.UserID)

	if item, err := get[Cheer](tx, key); err == nil {
		data.Bits += item.Bits
	}

	return set(tx, key, data)
}

func addFollow(tx *buntdb.Tx, data *Follow) error {
	key := fmt.Sprintf("follow:%s", data.UserID)

	if item, err := get[Follow](tx, key); err == nil {
		data.FollowedAt = item.FollowedAt
	}

	return set(tx, key, data)
}

func addRaid(tx *buntdb.Tx, data *Raid) error {
	key := fmt.Sprintf("raid:%s", data.UserID)

	if item, err := get[Raid](tx, key); err == nil && data.Viewers > item.Viewers {
		data.Viewers = item.Viewers
	}

	return set(tx, key, data)
}

func addSubscription(tx *buntdb.Tx, data *Subscription) error {
	key := fmt.Sprintf("subscription:%s", data.UserID)

	if item, err := get[Subscription](tx, key); err == nil && data.Months > item.Months {
		data.Months = item.Months
	}

	return set(tx, key, data)
}

func addSubscriptionGift(tx *buntdb.Tx, data *SubscriptionGift) error {
	key := fmt.Sprintf("subscription_gift:%s:%s", data.UserID, data.Tier)

	if item, err := get[SubscriptionGift](tx, key); err == nil {
		data.Total += item.Total
	}

	return set(tx, key, data)
}

func reply(w http.ResponseWriter, statusCode int, data string) error {
	w.WriteHeader(statusCode)

	if data == "" {
		return nil
	}

	if _, err := io.WriteString(w, data); err != nil {
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

	if _, err := subscribe(ctx, client, "channel.follow", "2", H{"broadcaster_user_id": userId, "moderator_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.raid", "1", H{"to_broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.subscribe", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.subscribe.message ", "1", H{"broadcaster_user_id": userId}); err != nil {
		return err
	}

	if _, err := subscribe(ctx, client, "channel.subscription.gift", "1", H{"broadcaster_user_id": userId}); err != nil {
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
				Name:    "api-server-address",
				EnvVars: []string{"API_SERVER_ADDRESS"},
				Usage:   "Address used by the server to listen",
				Value:   ":3000",
			},
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
			&cli.PathFlag{
				Name:    "database-path",
				EnvVars: []string{"DATABASE_PATH"},
				Usage:   "Path to the database",
				Value:   ":memory:",
			},
		},
		Action: func(ctx *cli.Context) error {
			db, err := buntdb.Open(ctx.Path("database-path"))

			if err != nil {
				return err
			}

			defer db.Close()

			db.CreateIndex("cheers", "cheer:*")
			db.CreateIndex("follows", "follow:*")
			db.CreateIndex("raids", "raid:*")
			db.CreateIndex("subscription_gifts", "subscription_gift:*")
			db.CreateIndex("subscriptions", "subscription:*")

			conf := &clientcredentials.Config{
				ClientID:     ctx.String("twitch-client-id"),
				ClientSecret: ctx.String("twitch-client-secret"),

				AuthStyle: twitch.Endpoint.AuthStyle,
				TokenURL:  twitch.Endpoint.TokenURL,
			}

			client := conf.Client(ctx.Context)
			router := bunrouter.New()

			knownMessageIds := make(map[string]struct{})

			router.GET("/", func(w http.ResponseWriter, req bunrouter.Request) error {
				var cheers []Cheer
				var follows []Follow
				var raids []Raid
				var subscriptionGifts []SubscriptionGift
				var subscriptions []Subscription

				err := db.View(func(tx *buntdb.Tx) error {
					cheers, _ = ascend[Cheer](tx, "cheers")
					follows, _ = ascend[Follow](tx, "follows")
					raids, _ = ascend[Raid](tx, "raids")
					subscriptionGifts, _ = ascend[SubscriptionGift](tx, "subscription_gifts")
					subscriptions, _ = ascend[Subscription](tx, "subscriptions")

					return nil
				})

				if err != nil {
					return err
				}

				data, err := encodeValue(H{
					"cheers":             cheers,
					"follows":            follows,
					"raids":              raids,
					"subscription_gifts": subscriptionGifts,
					"subscriptions":      subscriptions,
				})

				if err != nil {
					return err
				}

				return reply(w, http.StatusOK, data)
			})

			router.GET("/authorize/callback", func(w http.ResponseWriter, req bunrouter.Request) error {
				return reply(w, http.StatusOK, "You are now authorized, you can close this window.")
			})

			router.POST("/eventsub", func(w http.ResponseWriter, req bunrouter.Request) error {
				body, err := io.ReadAll(req.Body)

				if err != nil {
					return err
				}

				messageId := req.Header.Get("Twitch-Eventsub-Message-Id")

				if _, ok := knownMessageIds[messageId]; ok {
					return reply(w, http.StatusNoContent, "")
				}

				knownMessageIds[messageId] = struct{}{}

				if verifyMessage(ctx, req.Header, body) {
					res := gjson.ParseBytes(body)

					switch req.Header.Get("Twitch-Eventsub-Message-Type") {
					case "notification":
						log.Debug().Str("body", res.String()).Msg("Notification received")

						switch res.Get("subscription.type").String() {
						case "channel.cheer":
							data := &Cheer{
								UserID:    res.Get("event.user_id").String(),
								UserLogin: res.Get("event.user_login").String(),
								UserName:  res.Get("event.user_name").String(),
								Bits:      res.Get("event.bits").Int(),
							}

							err := db.Update(func(tx *buntdb.Tx) error {
								return addCheer(tx, data)
							})

							if err != nil {
								return err
							}

						case "channel.follow":
							data := &Follow{
								UserID:     res.Get("event.user_id").String(),
								UserLogin:  res.Get("event.user_login").String(),
								UserName:   res.Get("event.user_name").String(),
								FollowedAt: res.Get("event.followed_at").Time(),
							}

							err := db.Update(func(tx *buntdb.Tx) error {
								return addFollow(tx, data)
							})

							if err != nil {
								return err
							}

						case "channel.raid":
							data := &Raid{
								UserID:    res.Get("event.user_id").String(),
								UserLogin: res.Get("event.from_broadcaster_user_login").String(),
								UserName:  res.Get("event.from_broadcaster_user_name").String(),
								Viewers:   res.Get("event.viewers").Int(),
							}

							err := db.Update(func(tx *buntdb.Tx) error {
								return addRaid(tx, data)
							})

							if err != nil {
								return err
							}

						case "channel.subscribe", "channel.subscribe.message":
							if res.Get("event.is_gift").Bool() {
								break
							}

							data := &Subscription{
								UserID:    res.Get("event.user_id").String(),
								UserLogin: res.Get("event.user_login").String(),
								UserName:  res.Get("event.user_name").String(),
								Tier:      res.Get("event.tier").String(),
								Months:    1,
							}

							if months := res.Get("event.cumulative_months").Int(); months > 1 {
								data.Months = months
							}

							err := db.Update(func(tx *buntdb.Tx) error {
								return addSubscription(tx, data)
							})

							if err != nil {
								return err
							}

						case "channel.subscription.gift":
							data := &SubscriptionGift{
								UserID:    res.Get("event.user_id").String(),
								UserLogin: res.Get("event.user_login").String(),
								UserName:  res.Get("event.user_name").String(),
								Tier:      res.Get("event.tier").String(),
								Total:     res.Get("event.total").Int(),
							}

							err := db.Update(func(tx *buntdb.Tx) error {
								return addSubscriptionGift(tx, data)
							})

							if err != nil {
								return err
							}

						case "stream.offline":
							db.Update(func(tx *buntdb.Tx) error {
								_, _, err := tx.Set("offlineTime", time.Now().String(), &buntdb.SetOptions{
									TTL:     time.Duration(ctx.Int("session-reset-delay")) * time.Minute,
									Expires: true,
								})

								return err
							})

						case "stream.online":
							db.Update(func(tx *buntdb.Tx) error {
								_, err := tx.Get("offlineTime")

								if err == buntdb.ErrNotFound {
									return tx.DeleteAll()
								}

								return err
							})
						}

					case "webhook_callback_verification":
						return reply(w, http.StatusOK, res.Get("challenge").String())
					}

					return reply(w, http.StatusNoContent, "")
				}

				return reply(w, http.StatusForbidden, "")
			})

			if err := subscribeEvents(ctx, client); err != nil {
				return fmt.Errorf("an error occured while subscribing to events, please authorize the application first: https://id.twitch.tv/oauth2/authorize?response_type=code&client_id=%s&redirect_uri=%s/authorize/callback&scope=bits:read+channel:read:subscriptions+moderator:read:followers&force_verify=true", ctx.String("twitch-client-id"), ctx.String("api-base-url"))
			}

			return http.ListenAndServe(ctx.String("api-server-address"), router)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err)
	}
}
