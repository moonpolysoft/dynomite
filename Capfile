#A capfile for doing integration testing
role :dyn, "aa0-002-6-v1.u.powerset.com", 
          "aa0-002-6-v2.u.powerset.com",
          "aa0-002-6-v3.u.powerset.com",
          "aa0-002-6-v4.u.powerset.com"
          
role :tester, "aa0-009-6.u.powerset.com"
          
set :storage, (ENV["STORAGE"] || "fs_storage")

namespace :dynomite do
  desc <<-EOF
  Deploy dynomite via rsync, configure the data directories
  and launch one node per server.
  EOF
  task :default, :roles => :dyn do
    deploy.rsync
    deploy.compile
    deploy.data_reset
    deploy.start
  end
  
  task :bench, :roles => :tester do
    run "ruby dynomite/rlibs/distributed_bench.rb"
  end
  
  task :stop, :roles => :dyn do
    deploy.stop
  end
  
  namespace :deploy do
    task :rsync do
      username = ENV['USER']
      
      execute_on_servers(options) do |servers|
        servers.each do |server|
          puts %Q(rsync -avz -e ssh "./" "#{username}@#{server}:dynomite" --exclude ".git" --exclude "etest/log")
          `rsync -avz -e ssh "./" "#{username}@#{server}:dynomite" --exclude ".git" --exclude "etest/log"`
        end
      end
    end
    
    task :compile do
      run "cd dynomite && rake clean && rake native default"
    end

    task :data_reset, :roles => :dyn do
      run "rm -rf dyn-int-data dyn-int-log"
      run "mkdir dyn-int-data"
      run "mkdir dyn-int-log"
    end
    
    task :start, :roles => :dyn, :depends => [:data_reset] do
      execute_on_servers(options) do |servers|
        first_server = servers.shift
        shortname = first_server.to_s.split('.').first
        first = sessions[first_server]
        rest = servers.map {|s| sessions[s]}
        puts shortname
        Command.process("./dynomite/bin/dynomite start -s #{storage} -m ~/dyn-int-data -n 3 -r 2 -w 2 -l ~/dyn-int-log -d", [first], options.merge(:logger => logger))
        sleep(3)
        Command.process("./dynomite/bin/dynomite start -s #{storage} -m ~/dyn-int-data -j dynomite@#{shortname} -l ~/dyn-int-log -d", rest, options.merge(:logger => logger))
      end
    end
    
    task :stop, :roles => :dyn do
      run "./dynomite/bin/dynomite stop"
    end
  end
  
end