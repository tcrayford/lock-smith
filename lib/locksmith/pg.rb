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
      # We need to wrap the value for postgres
      if i > 2147483647
        -(-(i) & 0xffffffff)
      else
        i
      end
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

