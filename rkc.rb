# -*- encoding: utf-8 -*-

require 'optparse'
require 'redis'


pref = {"remote_host"=> "127.0.0.1", 
	"remote_port"=> 6379, 
	"local_host"=> "127.0.0.1", 
	"local_port"=>6379, 
}

key_list = ['hoge','fuga','piyo']

def filter_entities(redis,key_list)
  keys = redis.keys('*').sort
  entities = []
  key_list.each {|filter_key|
    keys.each {|key|
      if key.index(filter_key) != nil
        entities.push({'key' => key,'ttl' => redis.ttl(key),'value' => redis.get(key)})
      end
    }  
  }
  return entities;
end

def copy_entities(redis,entities)
  entities.each{ |entity|
    key = entity['key']
    if redis.exists(key) != true
      redis.set(key,entity['value'])
      redis.expire(key,-1)
    end 
 }
end

if __FILE__ == $0
  OptionParser.new do |opt|
    opt.on('-r [remote host]') {|v| pref["remote_host"] = v}
    opt.on('-p [remote port]') {|v| pref["remote_port"] = v.to_i} 
    opt.on('-l [local host]') {|v| pref["local_host"] = v}
    opt.on('-t [local port]') {|v| pref["local_port"] = v.to_i}
    opt.on('-e [key eternity]') {|v| pref["key_eternity"] = v.to_i}
    opt.version = '1.0.0'
    opt.parse!(ARGV)
  end
  p pref 

  remote_redis = Redis.new(:host => pref["remote_host"], 
			   :port => pref["remote_port"])
  local_redis = Redis.new(:host => pref["local_host"], 
	     	          :port => pref["local_port"])	
  entities = filter_entities(remote_redis,key_list)
  copy_entities(local_redis,entities); 

end