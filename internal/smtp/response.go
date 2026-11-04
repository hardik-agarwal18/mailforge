package smtp

const (
	ReplyServiceReady          = "220"
	ReplyServiceClosing        = "221"
	ReplyOK                    = "250"
	ReplyStartMailInput        = "354"
	ReplySyntaxError           = "501"
	ReplyBadSequence           = "503"
	ReplyCommandNotImplemented = "502"
	ReplyMailboxUnavailable    = "550"
)
