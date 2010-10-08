require 'rubygems'
require 'zipruby'
require 'json'
require 'mq'

t = Thread.new { EM.run }

AMQP.start(:host => "localhost",
           :user => "guest",
           :pass => "guest",
           :vhost => "/") do

  exch = MQ.topic('jobsched', :durable => true, :auto_delete => false)
  puts "Publishing patch..."
  exch.publish("""--- /tmp/test.txt	2010-10-08 14:46:09.000000000 -0500
+++ /tmp/test2.txt	2010-10-08 14:23:54.000000000 -0500
@@ -1,7 +1,8 @@
 This is a test file.
 This is a test file.
+NEW LINE ADDED!!!!!
+NEW LINE ADDED!!!!!
 This is a test file.
 This is a test file.
 This is a test file.
-This is a test file.
-This is a test file.
+NEW LINE ADDED!!!!!
""",
               :correlation_id => 'patch.test',
               :routing_key => 'jobsched.patch',
               :content_type => 'application/json',
               :headers => {
                   'security.key' => 'c16b2982-d30e-11df-962d-001ff3516166',
                   'file' => 'test.txt'
               })
  puts "Published message."

end

t.join(3)