FROM trueasync/php-true-async:0.7.0-beta.5-php8.6

RUN apt-get update && apt-get install -y \
    build-essential \
    autoconf \
    libtool \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Build and install phpredis from trueasync fork
RUN git clone --depth 1 --branch true-async https://github.com/true-async/phpredis.git /tmp/phpredis \
    && cd /tmp/phpredis \
    && phpize \
    && ./configure \
    && make -j$(nproc) \
    && make install \
    && echo 'extension=redis.so' > /etc/php.d/redis.ini \
    && rm -rf /tmp/phpredis