#!/usr/bin/env ruby -rbundler/setup -I.

require "pathname"
require "csv"
require "benchmark"

require "model"

class Line
  include ActiveModel::Model
  include ActiveModel::Serialization
  include Kinto::Model

  bucket :southpark
  collection :scripts

  attr_accessor :season, :episode, :character, :text

  def attributes
    {season: nil, episode: nil, character: nil, text: nil}
  end
end

PAGE_SIZE = 20

kinto   = Kinto.new(ENV['KINTO_URL'], ENV['KINTO_TOKEN'], File.open("/dev/null", "w"))
gateway = Kinto::Gateway.new(kinto)
models  = [Line]
gateway.ensure_buckets(*models)
gateway.ensure_collections(*models)

dataset = Pathname.getwd + "datasets/All-seasons.csv"
datum   = CSV.table(dataset) # XXX how to buffer instead of reading the whole file?

datum.take(1_000).each_slice(PAGE_SIZE) do |slice|
  lines = slice.map { |r| Line.new(season: r[:season],
                                   episode: r[:episode],
                                   character: r[:character],
                                   text: r[:line]) 
  }

  response = nil
  time = Benchmark.realtime do
    response = gateway.batch(lines)
  end

  results = response.responses
  if results.all? { |r| r["status"] == 201 }
    puts "HURRAY: #{time}"
  else
    raise
  end
end

binding.pry
