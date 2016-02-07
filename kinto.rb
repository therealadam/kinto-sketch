#!/usr/bin/env ruby
#
# A client for Kinto. No frills.
# ==============================

require "bundler/setup"
require "pry"

require "excon"
require "oj"
require "base64"

require "ostruct"

class Kinto

  def initialize(url, token)
    auth = Base64.urlsafe_encode64("token:#{token}")
    headers = {
      "Authorization": "Basic #{auth}",
      "Content-Type": "application/json",
      "Accept": "application/json",
    }
    @connection = Excon.new(url, headers: headers)
  end

  def healthcheck
    hash       = request(method: :get, path: '/v1/__heartbeat__')
    permission = hash["permission"]
    storage    = hash["storage"]
    cache      = hash["cache"]

    HealthcheckResponse.new(permission, storage, cache)
  end

  def info
    request(method: :get, path: '/v1/')
  end

  ## Buckets
  #
  # Not sure if one really needs this unless you're collaborating on
  # data across users.
  # ===================

  def default_bucket
    request(method: :get, path: '/v1/buckets')
  end

  def get_bucket(name)
    path = "/v1/buckets/#{name}"

    request(method: :get, path: path)
  end

  def all_buckets
    request(method: :get, path: '/v1/buckets')
  end

  def create_bucket(name)
    body = {"data" => {"id" => name}}
    json = Oj.dump(body)

    request(method: :post,
            path: '/v1/buckets',
            body: json,
            debug: true,
            expects: [200, 201],)
  end

  def get_bucket(name)
    request(method: :get,
            path: "/v1/buckets/#{name}",
            debug: true,
            expects: [200],)
  end

  # replace
  # delete

  ## Collections

  def create_collection(bucket: "default", name:)
    body = {"data" => {"id" => name}}
    json = Oj.dump(body)

    request(method: :post,
            path: "/v1/buckets/#{bucket}/collections",
            body: json,
            debug: true,
            expects: [200, 201],)
  end
  
  ## Records

  def create_record(bucket: "default", collection:, data:)
    body = {"data" => data}
    json = Oj.dump(body)

    request(method: :post,
            path: "/v1/buckets/#{bucket}/collections/#{collection}/records",
            body: json,
            debug: true,
            expects: [201],)
  end

  def update_record(bucket: "default", collection:, data:, kinto_id:)
    body = {"data" => data}
    json = Oj.dump(body)

    request(method: :put,
            path: "/v1/buckets/#{bucket}/collections/#{collection}/records/#{kinto_id}",
            body: json,
            debug: true,
            expects: [200],)
  end

  def delete_record(bucket: "default", collection:, kinto_id:)
    request(method: :delete,
            path: "/v1/buckets/#{bucket}/collections/#{collection}/records/#{kinto_id}",
            debug: true,
            expects: [200],)
  end

  protected

  def unjson(s)
    Oj.load(s)
  end

  def request(options)
    log("->", options)

    resp = @connection.request(options)
    body = resp.body
    hash = unjson(body)
    log("<-", hash)

    OpenStruct.new(hash)
  rescue => e
    log("err", e)
    log("err", e.response.body)
  end

  def log(prefix, options)
    STDOUT.puts [prefix, options].join(" ")
  end

  HealthcheckResponse = Struct.new(:permission, :storage, :cache)

end

url = ENV.fetch("KINTO_URL")
token = ENV.fetch("KINTO_TOKEN")
k = Kinto.new(url, token)

binding.pry if __FILE__ == $0
