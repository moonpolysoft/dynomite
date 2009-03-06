# Common build system
require 'fileutils'
require 'rubygems'
require 'rake'
require 'rake/clean'

VERSION = ENV["VERSION"]

ERLC_TEST_FLAGS = "-pa deps/eunit/ebin -I deps/eunit/include -DTEST"
ERLC_FLAGS = "+debug_info -W0 -I include -pa deps/mochiweb/ebin -I deps/mochiweb/include -pa deps/rfc4627/ebin -I deps/rfc4627/include -I gen-erl/ -o ebin"

CLEAN.include("ebin/*.beam")
CLEAN.include("c/*.o")
CLEAN.include("priv/*.so")

task :default => [:build_deps, :build_c_drivers] do
  puts "building #{ENV['TEST']}"
  sh "erlc  #{ERLC_FLAGS} #{ENV['TEST'] ? ERLC_TEST_FLAGS : ''} elibs/*.erl gen-erl/*.erl"
  if ENV['TEST']
    files = Dir["etest/*.erl"].select {|d| d !~ /^.*_test.erl$/}
    sh "erlc #{ERLC_FLAGS} #{ERLC_TEST_FLAGS} #{files.join(' ')}"
  end
  # Dir["templates/*"].each do |template|
  #   sh %Q(erl -pz ebin -noshell -eval 'erltl:compile("#{template}", [{outdir, "ebin"}, debug_info, show_errors, show_warnings])' -s erlang halt)
  # end
end

task :test_env => [:build_test_deps, :test_config] do
  puts "test env"
  ENV['TEST'] = 'test'
end

task :native do
  ERLC_FLAGS = "-smp +native #{ERLC_FLAGS}"
end

task :profile do
  ERLC_FLAGS = "-DPROF #{ERLC_FLAGS}"
end

task :run do
  sh %Q{erl -boot start_sasl +K true +A 128 -smp enable -pz ./ebin/ -sname local_console#{$$} -mnesia dir '"/tmp/mbd"' -noshell -run dynomite start}
end

task :build_dist => [:build_deps] do
  sh "erlc #{ERLC_FLAGS} elibs/*.erl"
  # Dir["templates/*"].each do |template|
  #   sh %Q(erl -pz ebin -noshell -eval 'erltl:compile("#{template}", [{outdir, "ebin"}, debug_info, show_errors, show_warnings])' -s erlang halt)
  # end
end


task :thrift_clients do
  sh "thrift --gen rb if/dynomite.thrift"
  sh "thrift -erl if/dynomite.thrift"
end 

task :release => [:default] do
  rel_file_contents = File.read("releases/dynomite.rel").gsub(/\?VERSION/, VERSION)

  release = "releases/dynomite-#{VERSION}"
  rel_file = "#{release}/dynomite_rel-#{VERSION}.rel"
  puts "preparing release #{release.inspect}"
  %w(ebin priv src include).each do |dir|
    FileUtils.mkdir_p("#{release}/#{dir}")
  end
  File.open(rel_file, 'w') do |f|
    f.print(rel_file_contents)
  end
  sh "cp -r elibs/* #{release}/src" rescue nil
  sh "cp -r ebin/* #{release}/ebin" rescue nil
  sh "cp -r priv/* #{release}/priv" rescue nil
  sh "cp -r include/* #{release}/include" rescue nil
  sh "cp -r deps/thrift/src/* #{release}/src" rescue nil
  sh "cp -r deps/thrift/ebin/* #{release}/ebin" rescue nil
  sh "cp -r deps/thrift/include/* #{release}/include" rescue nil
  sh "cp -r deps/thrift/priv/* #{release}/priv" rescue nil
  sh "cp -r deps/mochiweb/src/* #{release}/src" rescue nil
  sh "cp -r deps/mochiweb/ebin/* #{release}/ebin" rescue nil
  sh "cp -r deps/mochiweb/include/* #{release}/include" rescue nil
  sh "cp -r deps/mochiweb/priv/* #{release}/priv" rescue nil
  
  sh %Q(cd #{release} && erl -pa ./ebin -eval "systools:make_script(\\"dynomite_rel-#{VERSION}\\", [local])." -eval "systools:make_tar(\\"dynomite_rel-#{VERSION}\\")." -s init stop)
end

task :econsole do
  sh "erl +Bc +K true -smp enable -pz ./ebin -pz ./etest -pa ./deps/eunit/ebin -pa deps/rfc4627/ebin -pa deps/mochiweb/ebin -sname local_console_#{$$} -kernel"
end

task :stress => [:default] do
  sh "erl -boot start_sasl +Bc +K true -smp enable +A 128 -pz ./ebin -pz ./etest -pa ./deps/eunit/ebin -pa deps/rfc4627/ebin -pa deps/mochiweb/ebin -sname local_console_#{$$} -noshell -run dmerkle stress -run erlang halt"
end

task :console do
  sh "irb -I rlibs/"
end

task :test => [:test_env, :default] do
  mods = []
  mod_directives = ""
  env_peek = ENV['MOD'] || ENV['MODS'] || ENV['MODULE'] || ENV['MODULES']
  if env_peek
    mods = env_peek.split(",")
  else 
    mods = Dir["etest/*_test.erl"].map { |x| x.match(/etest\/(.*)_test.erl/)[1] }
  end
  mod_directives = mods.join(" ")
  priv = priv_dir()
  # -run #{ENV['MOD']} test
  sh %Q{erl -boot start_sasl +K true -smp enable -pz ./etest ./ebin -pa ./deps/eunit/ebin ./deps/mochiweb/ebin ./deps/rfc4627/ebin ./deps/thrift/ebin -sname local_console_#{$$} -noshell -priv_dir "#{priv}" -config test -s eunit test #{mod_directives} -run init stop}
  puts "-> Test logs in #{priv}"
end

task :coverage => [:test_env] do
  mods = []
  mod_directives = ""
  env_peek = ENV['MOD'] || ENV['MODS'] || ENV['MODULE'] || ENV['MODULES']
  if env_peek
    mods = env_peek.split(",")
  else 
    mods = Dir["etest/*_test.erl"].map { |x| x.match(/etest\/(.*)_test.erl/)[1] }
  end
  mod_directives = mods.join(', ')#map{|m| %Q(\\"#{m}\\")}.join(", ")
  priv = priv_dir()
  cmd = %Q{erl -boot start_sasl +K true -smp enable -pz etest -pa ./deps/eunit/ebin ./deps/mochiweb/ebin ./deps/rfc4627/ebin ./deps/thrift/ebin -sname local_console_#{$$} -noshell -priv_dir "#{priv}" -config test\
  -eval "\
     cover:compile_directory(\\"elibs\\", [{i,\\"include\\"},{i, \\"deps/eunit/include\\"},{d,'TEST'},{warn_format, 0}]), \
     T = fun(X) -> io:format(user, \\"~-20.s\\", [X]), X:test() end, \
     [T(X) || X <- [#{mod_directives}]], \
     F = fun(X) -> cover:analyse_to_file(X, \\"doc/\\" ++ atom_to_list(X) ++ \\"_coverage.html\\", [html]) end, \
     [F(X) || X <- [#{mod_directives}]]. \
     " -s init stop;}
  puts cmd
  sh cmd
  puts "-> Test logs in #{priv}"
end

task :docs do
  #files = (Dir["elibs/*.erl"] - ["elibs/json.erl"]).sort.map { |x| "\'../" + x + "\'"}.join(" ")
  #sh %|cd doc && erl -noshell -run edoc_run files #{files}|
  files = Dir["elibs/*.erl"].map { |x| "'../" + x + "'"}.join " "
  sh %|cd doc && erl -noshell -s init stop -run edoc files #{files}|
end

task :c_env do
  ERL = `which erl`
  ERLDIR = `awk -F= '/ROOTDIR=/ { print $2; exit; }' #{ERL}`.chomp
  ERTSBASE = `erl -noshell -noinput -eval 'io:format (\"~s\", [[ \"/\" ++ filename:join (lists:reverse ([ \"erts-\" ++ erlang:system_info (version) | tl (lists:reverse (string:tokens (code:lib_dir (), \"/\"))) ])) ]]).' -s erlang halt `.chomp
  ERL_INTERFACE = `ls #{ERLDIR}/lib`.split("\n").grep(/erl_interface/).last
  CPPFLAGS = "-I #{ERTSBASE}/include -I #{ERLDIR}/lib/#{ERL_INTERFACE}/include -Wall -g -O2 -fPIC -I./"
  LIBEI = "#{ERLDIR}/lib/#{ERL_INTERFACE}/lib/libei.a"
  if `uname` =~ /Linux/
    LDFLAGS = " -shared"
  else
    LDFLAGS = " -dynamic -bundle -undefined suppress -flat_namespace"
  end
end

task :build_deps do
  Dir["deps/*"].each do |dir|
    sh "cd #{dir} && make"
  end
end

task :build_test_deps do
  sh "erlc +debug_info -I include #{ERLC_TEST_FLAGS} -o etest etest/t.erl etest/mock_genserver.erl etest/mock.erl"
end

task :build_tarball => [:default, 'build'] do
  sh "wd=$(pwd) && cd ./.. && tar czvf dynomite/build/dynomite.tar.tgz --exclude dynomite/build ./dynomite && (cd \"$wd\")"
end

task :test_config do
  # ensure the test log dir exists
  priv = priv_dir()
  FileUtils.mkpath(priv)

  # write config file to configure sasl, etc to
  cfg = File.new("test.config", "w+")
  # write their error logs to the log files in log dir
  cfg.write(<<EOC)
[{kernel, 
  [{error_logger, {file, "#{priv}/kernel.log"}}
  ]},
 {sasl,
  [{sasl_error_logger, {file, "#{priv}/sasl.log"}}
  ]}
].
EOC
  cfg.close()
end

def priv_dir
  base = File.dirname(__FILE__)
  priv = File.join(base, "etest", "log", "#{$$}")
  return priv
end

DRIVERS = FileList['c/*_drv.c'].pathmap("%{c,priv}X.so")

<<<<<<< HEAD:Rakefile
directory "lib"
directory "build"
=======
directory "priv"
>>>>>>> lib -> priv:Rakefile

# task "priv/murmur_drv.c" => ["c/murmur.o"]

rule ".so" => ['%{priv,c}X.o', 'c/murmur.o', 'c/fnv.o'] do |t|
  puts "cc #{CPPFLAGS} #{LDFLAGS} -o #{t.name} #{t.prerequisites.join(' ')} #{LIBEI}"
  sh "cc #{CPPFLAGS} #{LDFLAGS} -o #{t.name} #{t.prerequisites.join(' ')} #{LIBEI}"
end

rule ".o" => ".c" do |t|
  puts "cc #{CPPFLAGS} -c -o #{t.name} #{t.source}"
  sh "cc #{CPPFLAGS} -c -o #{t.name} #{t.source}"
end


task :build_c_drivers => [:c_env, "priv"] + DRIVERS

