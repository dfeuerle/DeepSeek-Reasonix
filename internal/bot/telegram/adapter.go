// Package telegram implements Telegram Bot API adapter for Reasonix.
// Uses Long Polling (getUpdates) via go-telegram-bot-api/v5.
package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"reasonix/internal/bot"
	"reasonix/internal/config"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	platformName     = "telegram"
	longPollTimeout  = 60
	initialReconnect = 2 * time.Second
	maxReconnect     = 60 * time.Second
)

// New creates a Telegram Bot adapter with the given config.
func New(cfg config.TelegramBotConfig, logger *slog.Logger) bot.Adapter {
	return &adapter{
		cfg:    cfg,
		logger: logger.With("platform", platformName),
	}
}

type adapter struct {
	cfg    config.TelegramBotConfig
	logger *slog.Logger

	msgCh  chan bot.InboundMessage
	api    *tgbotapi.BotAPI
	cancel context.CancelFunc
	loopWG sync.WaitGroup
	closed bool
	mu     sync.Mutex
}

func (a *adapter) Platform() bot.Platform { return bot.PlatformTelegram }
func (a *adapter) Name() string           { return platformName }

func (a *adapter) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return fmt.Errorf("telegram adapter already closed")
	}
	if a.msgCh != nil {
		a.mu.Unlock()
		return fmt.Errorf("telegram adapter already started")
	}
	a.msgCh = make(chan bot.InboundMessage, 100)
	runCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.mu.Unlock()

	api, err := tgbotapi.NewBotAPI(a.cfg.BotToken)
	if err != nil {
		cancel()
		return fmt.Errorf("telegram bot init: %w", err)
	}
	a.api = api
	api.Debug = a.cfg.Debug

	botUser, err := api.GetMe()
	if err != nil {
		cancel()
		return fmt.Errorf("telegram getMe: %w", err)
	}
	a.logger.Info("telegram bot connected", "username", botUser.UserName, "id", botUser.ID)

	a.loopWG.Add(1)
	go a.pollLoop(runCtx)
	return nil
}

func (a *adapter) Stop() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()

	done := make(chan struct{})
	go func() {
		a.loopWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		a.logger.Warn("telegram adapter stop timeout")
	}
	return nil
}

func (a *adapter) Send(ctx context.Context, msg bot.OutboundMessage) (bot.SendResult, error) {
	chatID, err := strconv.ParseInt(msg.ChatID, 10, 64)
	if err != nil {
		return bot.SendResult{}, fmt.Errorf("telegram send: invalid chatID: %w", err)
	}

	tgMsg := tgbotapi.NewMessage(chatID, msg.Text)
	tgMsg.ParseMode = "MarkdownV2"

	if msg.ReplyToMsgID != "" {
		replyID, err := strconv.Atoi(msg.ReplyToMsgID)
		if err == nil {
			tgMsg.ReplyToMessageID = replyID
		}
	}

	sent, err := a.api.Send(tgMsg)
	if err != nil {
		return bot.SendResult{}, fmt.Errorf("telegram send: %w", err)
	}
	return bot.SendResult{MessageID: fmt.Sprintf("%d", sent.MessageID)}, nil
}

func (a *adapter) SendTyping(ctx context.Context, chatID string) error {
	cid, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return fmt.Errorf("telegram sendTyping: invalid chatID: %w", err)
	}
	ac := tgbotapi.NewChatAction(cid, "typing")
	_, err = a.api.Request(ac)
	if err != nil {
		return fmt.Errorf("telegram sendChatAction: %w", err)
	}
	return nil
}

func (a *adapter) Messages() <-chan bot.InboundMessage { return a.msgCh }

func (a *adapter) pollLoop(ctx context.Context) {
	defer a.loopWG.Done()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = longPollTimeout
	reconnectDelay := initialReconnect

	for {
		updates, err := a.api.GetUpdates(u)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			a.logger.Warn("telegram getUpdates error", "err", err, "retry_in", reconnectDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
				reconnectDelay *= 2
				if reconnectDelay > maxReconnect {
					reconnectDelay = maxReconnect
				}
				continue
			}
		}
		reconnectDelay = initialReconnect

		for _, update := range updates {
			if update.Message == nil {
				continue
			}
			msg := update.Message
			chatType := resolveChatType(msg.Chat)
			chatID := fmt.Sprintf("%d", msg.Chat.ID)

			inbound := bot.InboundMessage{
				Platform:  bot.PlatformTelegram,
				ChatType:  chatType,
				ChatID:    chatID,
				UserID:    fmt.Sprintf("%d", msg.From.ID),
				UserName:  formatUserName(msg.From),
				Text:      msg.Text,
				MessageID: fmt.Sprintf("%d", msg.MessageID),
			}

			u.Offset = update.UpdateID + 1

			select {
			case a.msgCh <- inbound:
			case <-ctx.Done():
				return
			default:
				a.logger.Warn("telegram inbound channel full, dropping message")
			}
		}
	}
}

func resolveChatType(chat *tgbotapi.Chat) bot.ChatType {
	switch chat.Type {
	case "private":
		return bot.ChatDM
	case "group", "supergroup":
		return bot.ChatGroup
	case "channel":
		return bot.ChatDirect
	default:
		return bot.ChatDM
	}
}

func formatUserName(u *tgbotapi.User) string {
	name := u.FirstName
	if u.LastName != "" {
		name += " " + u.LastName
	}
	if u.UserName != "" {
		name = "@" + u.UserName
	}
	return name
}

var _ bot.Adapter = (*adapter)(nil)
