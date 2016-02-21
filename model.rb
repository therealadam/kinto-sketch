#!/usr/bin/env ruby -rbundler/setup -I.

require "kinto"
require "active_model"

##
# Declarative model helpers for working with records in Kinto.
#
# Including classes should implement #attributes, as per
# ActiveModel::Serialization.
module Kinto::Model
  extend ActiveSupport::Concern

  included do
    unless self.ancestors.include?(ActiveModel::Model)
      raise ArgumentError.new("Expected #{self} to include ActiveModel::Model")
    end

    unless self.ancestors.include?(ActiveModel::Serialization)
      raise ArgumentError.new("Expected #{self} to include ActiveModel::Serialization")
    end

    # unless self.instance_methods.include?(:attributes)
    #   raise ArgumentError.new("Expected #{self} to implement #attributes")
    # end

    cattr_accessor :kinto_params
    self.kinto_params = {
      bucket: nil,
      collection: nil,
    }

    attr_accessor :id, :last_modified # Kinto-assigned
  end

  class_methods do
    # XXX maybe hang bucket off the Gateway since it is likely to vary per
    # operational configuration and not data model
    def bucket(name)
      self.kinto_params[:bucket] = name.to_s
    end

    def collection(name)
      self.kinto_params[:collection] = name.to_s
    end
  end

  def to_kinto_params
    payload = serializable_hash.stringify_keys

    kinto_params.merge(data: payload)
  end

  def to_operation
    bucket = self.kinto_params[:bucket]
    collection = self.kinto_params[:collection]

    # TODO updates/deletes
    {
      method: "POST",
      path: "/buckets/#{bucket}/collections/#{collection}/records",
      body: {"data" => serializable_hash.stringify_keys},
    }
  end

  def set_kinto_attributes(data)
    self.id = data["id"]
    self.last_modified = data["last_modified"] # XXX parse
  end
end

##
#  If you're using Kinto::Model, you probably want to run all interactions
#  through this gateway class. Which I'm not sure if it should exist or not.
#
#  K::G expects model objects that include the following modules:
#
#  * Kinto::Model
#  * ActiveModel::Model
#  * ActiveModel::Serialization
#
#  Your mileage with libraries that implement those duck types may vary.
class Kinto::Gateway

  def initialize(connection)
    @connection = connection
  end

  def ensure_buckets(*models)
    buckets = models.
      map(&:kinto_params).
      map { |h| h[:bucket] }.
      uniq

    buckets.each do |b|
      @connection.create_bucket(b) # IDEMPOTENTCY is wonderful
    end
  end

  def ensure_collections(*models)
    models.each do |m|
      params = m.kinto_params
      bucket = params[:bucket]
      collection = params[:collection]

      @connection.create_collection(bucket: bucket, name: collection)
    end
  end

  def create_record(model)
    params = model.to_kinto_params

    resp = @connection.create_record(params)

    model.set_kinto_attributes(resp.data)
    model
  end

  def update_record(model)
    params = model.
      to_kinto_params.
      merge(kinto_id: model.id)

    resp = @connection.update_record(params)
    model.set_kinto_attributes(resp.data)
    model
  end

  def delete_record(model)
    params = model.
      to_kinto_params.
      merge(kinto_id: model.id).
      tap { |p| p.delete(:data) }

    resp = @connection.delete_record(params)
  end

  def batch(models)
    operations = models.map(&:to_operation)
    resp = @connection.batch(operations)
  end

end

class Post
  include ActiveModel::Model
  include ActiveModel::Serialization
  include Kinto::Model

  bucket :blog
  collection :posts

  attr_accessor :title, :body, :author_id

  def attributes
    {title: nil, body: nil, author_id: nil}
  end
end

class Author
  include ActiveModel::Model
  include ActiveModel::Serialization
  include Kinto::Model

  bucket :blog
  collection :authors

  attr_accessor :id, :name, :url

  def attributes
    {name: nil, url: nil}
  end
end

k = Kinto.new(ENV['KINTO_URL'], ENV['KINTO_TOKEN'])
kg = Kinto::Gateway.new(k)
models = [Post, Author]

if __FILE__ == $0
  ## ensure the necessary bucket and collections are present
  kg.ensure_buckets(*models)
  kg.ensure_collections(*models)

  author = Author.new(name: "Joe Lamer", url: "http://example.com")
  saved_author = kg.create_record(author)

  post = Post.new(title: "First!", body: "lame", author_id: saved_author.id)
  saved_post = kg.create_record(post)

  saved_post.body = "Not so lame, now"
  updated_post = kg.update_record(saved_post)

  # result = kg.delete_record(updated_post)
  puts updated_post.id

  binding.pry
end
