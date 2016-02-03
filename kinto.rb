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
    @connection = Excon.new(url, headers: {"Authorization": "Basic #{auth}"})
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
    request(method: :post, path: '/v1/buckets')
  end

  # replace
  # delete

  ## Collections

  def create_collection(bucket: "default", name:)
    request(method: :post, path: "/v1/buckets/#{bucket}/collections")
  end
  
  ## Records

  def create_record(bucket: "default", collection:, data:)
    body = {"data" => data}
    json = Oj.dump(body)

    request(method: :post,
            path: "/v1/buckets/#{bucket}/collections/#{collection}/records",
            headers: {"Content-Type" => "application/json"},
            body: json,
            debug: true,
            expects: [201],)
  end

  protected

  def unjson(s)
    Oj.load(s)
  end

  def request(options)
    resp = @connection.request(options)
    body = resp.body
    hash = unjson(body)

    OpenStruct.new(hash)
  end

  HealthcheckResponse = Struct.new(:permission, :storage, :cache)

end

url = ENV.fetch("KINTO_URL")
token = ENV.fetch("KINTO_TOKEN")
k = Kinto.new(url, token)

binding.pry
