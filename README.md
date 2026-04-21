# Thrun

Async queue worker for PHP built on [TrueAsync](https://github.com/true-async) - alternative PHP core that implements true asynchrony by modifying the Zend core,
I/O libraries, database and socket handling.

## Goal

The fastest async queue worker for PHP - one worker process that handles both IO-bound and CPU-bound tasks efficiently.
Uses real OS threads instead of forked processes, consumes significantly less memory,
and aims to outperform Symfony Messenger and Laravel Horizon.

## Requirements

- TrueAsync PHP 8.6+

## Installation

```bash
composer require yangusik/thrun
```

> Package in development and may change name

## License

MIT
