require 'zlib'
require 'uri'
require 'pg'

module Locksmith
  module Pg
  extend self
  BACKOFF = 0.5

    def lock_space
      @lock_space ||= (ENV['LOCKSMITH_PG_LOCK_SPACE'] || '-2147483648').to_i
    end

    def lock(name, lspace=lock_space)
      i = pg_lock_number(name)
      result = nil
      begin
        sleep(BACKOFF) until write_lock(i, lspace)
        if block_given?
          result = yield
        end
        return result
      ensure
        release_lock(i, lspace)
      end
    end

    def pg_lock_number(name)
      i = Zlib.crc32(name)
      # Anything bigger than 2147483647 needs to be wrapped
      # negative, so treat the left most bit as the +/- flag
      if i == 2147483648
        # the wrap trick doesn't work for 2147483648
        # as it would be 0
        i = -i
      elsif i > 2147483647
        i = - "#{i.to_s(2)[1..-1]}".to_i(2)
      end
      i
    end

    def write_lock(i, lspace)
      r = conn.exec("select pg_try_advisory_lock($1,$2)", [lspace,i])
      r[0]["pg_try_advisory_lock"] == "t"
    end

    def release_lock(i, lspace)
      conn.exec("select pg_advisory_unlock($1,$2)", [lspace,i])
    end

    def conn=(conn)
      @conn = conn
    end

    def conn
      @conn ||= PG::Connection.open(
        dburl.host,
        dburl.port || 5432,
        nil, '', #opts, tty
        dburl.path.gsub("/",""), # database name
        dburl.user,
        dburl.password
      )
    end

    def dburl
      URI.parse(ENV["DATABASE_URL"])
    end

    def log(data, &blk)
      Log.log({:ns => "postgresql-lock"}.merge(data), &blk)
    end

  end
end

