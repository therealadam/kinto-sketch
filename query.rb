#!/usr/bin/env ruby -rbundler/setup -I.

require "kinto"
require "model"

# https://kinto.readthedocs.org/en/latest/api/1.x/cliquet/resource.html#get-collection

class Kinto::Query

  def initialize(connection, bucket, collection)
    @bucket = bucket
    @collection = collection
    @connection = connection
  end

  def get(record_id: nil, record_ids: nil, limit: nil)
    if record_id
      @connection.get_record(bucket: @bucket, collection: @collection,
                             record_id: record_id)
    elsif record_ids
      @connection.get_collection(bucket: @bucket, collection: @collection,
                                 query: {in_id: record_ids.join(",")})
    elsif limit
      @connection.get_collection(bucket: @bucket, collection: @collection,
                                 query: {_limit: limit})
    end
  end

  def filter(query)
    fields = query.
      delete(:fields).
      map(&:to_s).
      join(",")
    limit = query.delete(:limit)
    sort = query.delete(:sort)
    generated_query = query.merge(
      _fields: fields,
      _limit: limit,
      _sort: sort
    )

    @connection.get_collection(bucket: @bucket, collection: @collection,
                               query: generated_query)
  end

end

bucket = "southpark"
collection = "scripts"
k = Kinto.new(ENV['KINTO_URL'], ENV['KINTO_TOKEN'])
kq = Kinto::Query.new(k, bucket, collection)

recs = kq.get(limit: 3)
record_ids = recs.data.map { |r| r["id"] }
kq.get(record_ids: record_ids)

more_records = kq.filter(fields: [:text, :character],
                         limit: 10,
                         min_season: 10,
                         sort: "-episode")

# paginate

binding.pry if __FILE__ == $0
