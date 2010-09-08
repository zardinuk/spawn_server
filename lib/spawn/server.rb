require 'logger'
require 'spawn/task'

module Spawn
  class Server
    POLLING_INTERVAL = 60

    attr_accessor :pids
    attr_accessor :tasks
    attr_accessor :interval
    attr_accessor :start_times

    def initialize(tasks, interval=POLLING_INTERVAL)
      puts "Initializing spawn server"

      self.pids = {}
      self.start_times = {}
      self.tasks = tasks
      self.interval = interval

      # trap INT for debugging, in production we want to leave children active
      Signal.trap('INT') do
        process_list = self.pids.map{|k,v| "#{k} (#{v})" }.join(', ')
        print " *** Interrupt received, killing #{process_list} ***\n"
        Process.kill('INT', *self.pids.values.map{|h| h.keys }.flatten )
        exit(1)
      end

      start_tasks
    end

    def logger
      if instance_variables.include?(:@logger)
        @logger
      else
        @logger = Logger.new(File.open('log/spawn_server.log', 'w'))
        @logger.level = Logger::DEBUG
        @logger
      end
    end

    def start_tasks
      puts self.tasks.inspect

      # first load in pid files
      self.tasks.each do |id, params|
        self.pids[id] = {}

        params[:max_threads].times do |thread_num|
          thread_pid_file = pid_file(id, thread_num+1)

          puts thread_pid_file

          if File.exists?(thread_pid_file)
            pid = File.open(thread_pid_file).read.to_i
            if (Process.kill(0, pid) rescue false)
              self.pids[id][pid] = File.stat(thread_pid_file).mtime
            else
              File.unlink(thread_pid_file) rescue nil
            end
          end
        end

        if params[:reload]
          case params[:reload]
          when :all
            self.pids[id].each do |pid,mtime|
              process_stop(id, pid, true)
            end
          when :parent
            self.pids[id].each do |pid,mtime|
              process_stop(id, pid, false)
            end
          else
            puts "Unrecognized value for :reload parameter in #{id}: #{params[:reload]}"
          end
        end
      end

      loop do
        Thread.new do
          self.tasks.each do |id, params|
            puts "Checking if #{id} is running"

            if (!process_running?(id, params[:max_threads]))
              puts "It's not, starting..."
              # not running, or not enough running, start it...
              process_start(id, params) do
                if params[:task].is_a?(String)
                  Rake::Task[params[:task]].invoke
                elsif params[:task].is_a?(Proc)
                  puts "Initializing task #{id}"
                  params[:task].call
                end
                exit
              end
            end

            # check if the processes are running too long, this needs to be done after "process_running" is called so that the pid list is cleared, it's not optimal this way but good enough for now
            self.pids[id].each do |pid,mtime|
              if params[:max_life] && params[:max_life] < time_running(id,pid)
                puts "Before process stop (#{pid}):\n#{process_list}"
                process_stop(id,pid)
                puts "After process stop (#{pid}):\n#{process_list}"

                # BackgroundCheck.deliver_process_max_life(id, time_running(id,pid), params[:max_life]) rescue nil
              end
            end
          end
        end

        # Run the above code in a thread so that the interval is more consistent
        sleep(self.interval)
      end
    end

    def process_list
      `ps uxf`
    end

    def time_running(id, pid)
      if self.pids[id][pid]
        Time.now - self.pids[id][pid]
      end
    end

    def pid_file(id, num=1)
      "tmp/#{id}.#{num}.pid"
    end

    def process_running?(id, quota=1)
      running = 0
      process_list = self.pids[id]
      if process_list && process_list.is_a?(Hash)
        process_list.each do |pid,mtime|
          # do this first to wipe the defunct children, won't block
          # Process.wait(pid, Process::WNOHANG)
        
          if (Process.kill(0, pid) rescue false)
            running += 1
          else
            # look for this pid file and remove it
            self.tasks[id][:max_threads].times do |i|
              thread_pid_file = pid_file(id,i+1)
              if File.exists?(thread_pid_file)
                existing_pid = File.open(thread_pid_file).read
                if existing_pid.to_i == pid.to_i
                  File.unlink(thread_pid_file)
                end
              end
            end
            self.pids[id].delete(pid)
          end
        end
      end

      return (running >= quota)
    end

    def process_stop(id, pid, recursive=true)
      return unless pid

      if recursive
        # recursive, look through all the stat files for this one's child processes and call recursively
        Dir.glob('/proc/*/status').each do |stat_file|
          File.open(stat_file).each do |line|
            if line =~ /^PPid:\s*#{pid}$/
              process_stop(id, stat_file.match(/[0-9]+/)[0].to_i)
              break
            end
          end
        end
      end

      puts "Stopping process #{pid} (#{id})"
      Process.kill(9, pid) rescue nil
    end

    def process_start(id, params={})
      pid = Spawn::Task.new do
        CACHE.reset if defined?(CACHE)
        ActiveRecord::Base.clear_active_connections! if defined?(ActiveRecord::Base)
        yield
      end
      pid = pid.handle.to_i

      puts "Started process #{pid} (#{id})"
      begin
        params[:max_threads].times do |thread_num|
          thread_pid_file = pid_file(id, thread_num+1)
          if File.exists?(thread_pid_file)
            # this pid must be running still, cause we must have just checked
          else
            File.open(thread_pid_file, 'w'){|f| f << "#{pid}" }
            break
          end
        end
      rescue Exception => e
        # email this log message
        puts e.message
      end
      self.pids[id][pid] = Time.now

      if params[:priority]
        Process.setpriority(Process::PRIO_PROCESS, pid, params[:priority])
      end
    end
    
  end
end
