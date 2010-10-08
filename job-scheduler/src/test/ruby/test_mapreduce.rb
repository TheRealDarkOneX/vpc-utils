require 'rubygems'
require 'zipruby'
require 'json'
require 'mq'

t = Thread.new { EM.run }

AMQP.start(:host => "localhost",
           :user => "guest",
           :pass => "guest",
           :vhost => "/") do

  results = MQ.queue('mapred.test').subscribe do |hdrs, body|
    print "body: #{body}"
    EM.stop_event_loop
  end

  exch = MQ.topic('jobsched', :durable => true, :auto_delete => false)
  puts "Publishing message..."
  exch.publish(JSON.dump({"datasource" => "test",
                          "sql" => "plugin:sql_handler_mapreduce.groovy?sql=select+*+from+mapred.test&src=test_mapred.groovy",
                          "params" => []}),
               :correlation_id => 'mapred.test',
               :routing_key => 'jobsched.sql',
               :content_type => 'application/json',
               :reply_to => 'mapred.test',
               :headers => {
                   'security.key' => 'ca560418-ce31-11df-8a2d-001e52fffe45'
               })
  puts "Published message."

end

t.join(15)