FROM jruby:9.1-jdk

RUN mkdir -p /var/lib/vmpooler
WORKDIR /var/lib/vmpooler

ADD Gemfile /var/lib/vmpooler
ADD Gemfile.lock /var/lib/vmpooler
RUN bundle install --system

RUN ln -s /opt/jruby/bin/jruby /usr/bin/jruby

RUN echo "deb http://httpredir.debian.org/debian jessie main" >/etc/apt/sources.list.d/jessie-main.list
RUN apt-get update
RUN apt-get install -y redis-server

COPY . /var/lib/vmpooler

ENTRYPOINT \
    /etc/init.d/redis-server start \
    && /var/lib/vmpooler/scripts/vmpooler_init.sh start \
    && while [ ! -f /var/log/vmpooler.log ]; do sleep 1; done ; \
    tail -f /var/log/vmpooler.log
