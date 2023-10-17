require 'json'

filename_re = /^R([0-9]+)-([0-9]+)-([0-9])+.sh.o[0-9]+$/
line_re = /INFO tag: IOPS iops=([0-9]+)$/
iops = {}
Dir.glob('*.sh.o*').each do |file|
  captured = filename_re.match(file).captures
  server_thread_count = captured[1].to_i
  client_thread_count = captured[2].to_i
  client_task_count = captured[3].to_i
  unless iops.key? server_thread_count
    iops[server_thread_count] = {}
  end
  unless iops[server_thread_count].key? client_thread_count
    iops[server_thread_count][client_thread_count] = {}
  end
  unless iops[server_thread_count][client_thread_count].key? client_task_count
    iops[server_thread_count][client_thread_count][client_task_count] = {}
  end

  iops_series = []

  open(file).each do |line|
    captured = line_re.match(line)
    if captured != nil
      iops_series.push (captured[1].to_i)
    end
  end

  iops[server_thread_count][client_thread_count][client_task_count] = iops_series
end

print JSON.pretty_generate(iops)
