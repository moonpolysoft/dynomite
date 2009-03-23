#Define your servers in servers.rb like so:


# role :dyn, "server1", 
#           "server2",
#           "server3",
#           "server4"
#           
# role :tester, "server5"
          
load "servers.rb"
          
set :storage, (ENV["STORAGE"] || "dets_storage")

namespace :dynomite do
  desc <<-EOF
  Deploy dynomite via rsync, configure the data directories
  and launch one node per server.
  EOF
  task :default, :roles => :dyn do
    deploy.rsync
    deploy.compile
    deploy.data_reset
    deploy.config
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
      run "cd dynomite && rake clean && rake native profile default"
    end

    task :data_reset, :roles => :dyn do
      run "rm -rf /bfd/dyn-int-data dyn-int-log/*"
      run "mkdir -p /bfd/dyn-int-data"
      run "mkdir -p dyn-int-log"
    end
    
    task :config, :roles => :dyn do
      configuration = ENV["CONFIG"] || "dist_config.json"
      contents = File.read(configuration)
      put(contents, "./dynomite/config.json")
    end
    
    task :start, :roles => :dyn, :depends => [:data_reset, :config] do
      execute_on_servers(options) do |servers|
        first_server = servers.shift
        shortname = first_server.to_s.split('.').first
        first = sessions[first_server]
        rest = servers.map {|s| sessions[s]}
        puts shortname
        if ENV["SERVERS"]
          rest = rest[0...(ENV["SERVERS"].to_i-1)]
        end
        Command.process("./dynomite/bin/dynomite start -c config.json -l ~/dyn-int-log -d", [first], options.merge(:logger => logger))
        sleep(3)
        Command.process("./dynomite/bin/dynomite start -c config.json -j dynomite@#{shortname} -l ~/dyn-int-log -d", rest, options.merge(:logger => logger))
      end
    end
    
    task :stop, :roles => :dyn do
      run "./dynomite/bin/dynomite stop"
    end
  end
  
end

def put_file(path, remote_path, options = {})
  execute_on_servers(options) do |servers|
    servers.each do |server|
      logger.info "uploading #{File.basename(path)} to #{server}"
      sftp = sessions[server].sftp
      sftp.connect unless sftp.state == :open
      sftp.put_file path, remote_path
      logger.debug "done uploading #{File.basename(path)} to #{server}"
    end
  end
end