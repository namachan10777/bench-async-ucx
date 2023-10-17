require 'erb'

template = IO.read("template.sh.erb")
server_thread_count_max = 8
client_thread_count_max = 8
client_actual_task_count_max = 1024
nodes = 2

template = ERB.new(template)

server_thread_count = 1
while server_thread_count <= server_thread_count_max
  client_thread_count = 1
  while client_thread_count <= client_thread_count_max
    client_task_count = 1
    while client_thread_count * client_task_count <= client_actual_task_count_max
      IO.write("#{server_thread_count}-#{client_thread_count}-#{client_task_count}.sh", template.result)
      client_task_count *= 8
    end
    client_thread_count *= 2
  end
  server_thread_count *= 2
end
