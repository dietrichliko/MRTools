"""My take at logging with click

Inspired by https://github.com/click-contrib/click-log

Example:

    import logging

    import click
    import mrtools.clicklog as clicklog

    clicklog.basicConfig()
    log = logging.getLogger(__package__)

    @click.command()
    @clicklog.log_level_option(log)
    def main():

Dietrich Liko
"""

import logging

import click


def log_level_option(logger: logging.Logger, *names, **kwargs):

    if not names:
        names = ("--log-level", "-l")

    kwargs.setdefault("default", "INFO")
    kwargs.setdefault("metavar", "LVL")
    kwargs.setdefault("expose_value", False)
    kwargs.setdefault("help", "CRITICAL, ERROR, WARNING, INFO or DEBUG")
    kwargs.setdefault("is_eager", True)
    kwargs.setdefault(
        "type",
        click.Choice(
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
        ),
    )

    def decorator(f):
        def _set_level(ctx, param, value):
            x = getattr(logging, value.upper(), None)
            if x is None:
                raise click.BadParameter(
                    "Must be CRITICAL, ERROR, WARNING, INFO or DEBUG, not {}"
                )
            logger.setLevel(x)

        return click.option(*names, callback=_set_level, **kwargs)(f)

    return decorator


_CLICK_STYLE = {
    logging.DEBUG: {
        "levelname": dict(fg="white", reverse=True),
        "message": dict(fg="white"),
    },
    logging.INFO: {
        "levelname": dict(fg="green", reverse=True),
        "message": dict(fg="green"),
    },
    logging.WARNING: {
        "levelname": dict(fg="yellow", reverse=True),
        "message": dict(fg="yellow"),
    },
    logging.ERROR: {
        "levelname": dict(fg="red", reverse=True),
        "message": dict(fg="red"),
    },
    logging.CRITICAL: {
        "levelname": dict(fg="red", reverse=True),
        "message": dict(fg="red"),
    },
}


class ClickHandler(logging.Handler):
    """
    A handler class which writes logging records, appropriately formatted,
    using click.echo
    """

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """

        try:
            msg = self.format(record)
            click.echo(msg)
        except Exception:
            self.handleError(record)


class ClickFormatter(logging.Formatter):
    """Formatter for the Click handler

    Formatter instances are used to convert a LogRecord to text.
    Formatters need to know how a LogRecord is constructed. They are
    responsible for converting a LogRecord to (usually) a string which can
    be interpreted by either a human or an external system. The base Formatter
    allows a formatting string to be specified. If none is supplied, the
    style-dependent default value, "%(message)s", "{message}", or
    "${message}", is used.
    The Formatter can be initialized with a format string which makes use of
    knowledge of the LogRecord attributes - e.g. the default value mentioned
    above makes use of the fact that the user's message and arguments are pre-
    formatted into a LogRecord's message attribute. Currently, the useful
    attributes in a LogRecord are described by:
    %(name)s            Name of the logger (logging channel)
    %(levelno)s         Numeric logging level for the message (DEBUG, INFO,
                        WARNING, ERROR, CRITICAL)
    %(levelname)s       Text logging level for the message ("DEBUG", "INFO",
                        "WARNING", "ERROR", "CRITICAL")
    %(pathname)s        Full pathname of the source file where the logging
                        call was issued (if available)
    %(filename)s        Filename portion of pathname
    %(module)s          Module (name portion of filename)
    %(lineno)d          Source line number where the logging call was issued
                        (if available)
    %(funcName)s        Function name
    %(created)f         Time when the LogRecord was created (time.time()
                        return value)
    %(asctime)s         Textual time when the LogRecord was created
    %(msecs)d           Millisecond portion of the creation time
    %(relativeCreated)d Time in milliseconds when the LogRecord was created,
                        relative to the time the logging module was loaded
                        (typically at application startup time)
    %(thread)d          Thread ID (if available)
    %(threadName)s      Thread name (if available)
    %(process)d         Process ID (if available)
    %(message)s         The result of record.getMessage(), computed just as
                        the record is emitted
    """

    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style="%",
        validate=True,
        *,
        click_style=_CLICK_STYLE,
    ):

        super().__init__(fmt, datefmt, style, validate)
        # super().__init__(fmt, datefmt, style, validate, defaults=defaults) # python 3.10
        self.click_style = click_style

    def formatMessage(self, record):

        try:
            for name, style in self.click_style[record.levelno].items():
                value = getattr(record, name)
                setattr(record, name, click.style(value, **style))
        except KeyError:
            pass

        return self._style.format(record)


def basicConfig(**kwargs):
    """
    Do basic configuration for the logging system.

    This function does nothing if the root logger already has handlers
    configured, unless the keyword argument *force* is set to ``True``.
    It is a convenience method intended for use by simple scripts
    to do one-shot configuration of the logging package.

    The default behaviour is to create a ClickHandler which writes to
    sys.stderr, set a formatter using the BASIC_FORMAT format string, and
    add the handler to the root logger.

    A number of optional keyword arguments may be specified, which can alter
    the default behaviour.

    format      Use the specified format string for the handler.
    datefmt     Use the specified date/time format.
    style       If a format string is specified, use this to specify the
                type of format string (possible values '%', '{', '$', for
                %-formatting, :meth:`str.format` and :class:`string.Template`
                - defaults to '%').
    clickstyle   A dictionary defining click.echo attributes
    level       Set the root logger level to the specified level.
    force       If this keyword  is specified as true, any existing handlers
                attached to the root logger are removed and closed, before
                carrying out the configuration as specified by the other
                arguments.


    """
    # Add thread safety in case someone mistakenly calls
    # basicConfig() from multiple threads
    logging._acquireLock()
    try:
        force = kwargs.pop("force", False)
        if force:
            for h in logging.root.handlers[:]:
                logging.root.removeHandler(h)
                h.close()
        if len(logging.root.handlers) == 0:
            handlers = [ClickHandler()]
            dfs = kwargs.pop("datefmt", None)
            style = kwargs.pop("style", "%")
            if style not in logging._STYLES:
                raise ValueError(
                    "Style must be one of: %s" % ",".join(logging._STYLES.keys())
                )
            fs = kwargs.pop("format", logging._STYLES[style][1])
            click_style = kwargs.pop("clickstyle", _CLICK_STYLE)
            fmt = ClickFormatter(fs, dfs, style, click_style=click_style)
            for h in handlers:
                if h.formatter is None:
                    h.setFormatter(fmt)
                logging.root.addHandler(h)
            level = kwargs.pop("level", None)
            if level is not None:
                logging.root.setLevel(level)
            if kwargs:
                keys = ", ".join(kwargs.keys())
                raise ValueError("Unrecognised argument(s): %s" % keys)
    finally:
        logging._releaseLock()
