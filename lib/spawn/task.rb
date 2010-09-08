module Spawn
  class Task
    attr_accessor :handle

    # socket to close in child process
    @@resources = []

    # set the resource to disconnect from in the child process (when forking)
    def self.resource_to_close(resource)
      @@resources << resource
    end

    # close all the resources added by calls to resource_to_close
    def self.close_resources
      @@resources.each do |resource|
        resource.close if resource && resource.respond_to?(:close) && !resource.closed?
      end
    end

    # Spawns a long-running section of code and returns the ID of the spawned process.
    def initialize(options={})
      self.handle = fork_it(options) { yield }
    end

    def wait(sids = [])
      # wait for all threads and/or forks (if a single sid passed in, convert to array first)
      Array(sids).each do |sid|
        if sid.type == :thread
          sid.handle.join()
        else
          begin
            Process.wait(sid.handle)
          rescue
            # if the process is already done, ignore the error
          end
        end
      end
    end

    protected
    def fork_it(options)
      child = fork do
        begin
          start = Time.now

          # set the nice priority if needed
          Process.setpriority(Process::PRIO_PROCESS, 0, options[:nice]) if options[:nice]

          # disconnect from the listening socket, et al
          self.class.close_resources

          # run the block of code that takes so long
          yield

        rescue => ex
          $stderr.puts "spawn> Exception in child[#{Process.pid}] - #{ex.class}: #{ex.message} #{ex.backtrace}"
        ensure
          exit!(0)
        end
      end
    
      # detach from child process (parent may still wait for detached process if they wish)
      Process.detach(child)

      return child
    end
  end
end
