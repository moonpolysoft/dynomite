# Common build system
require 'rubygems'
require 'rake'

ERLC_TEST_FLAGS = "-pa deps/eunit/ebin -I deps/eunit/include -DTEST"
ERLC_FLAGS = "+debug_info -W0 -I include -pa deps/mochiweb/ebin -I deps/mochiweb/include -pa deps/rfc4627/ebin -I deps/rfc4627/include -I gen-erl/ -o ebin"

task :default => [:build_deps] do
  puts "building #{ENV['TEST']}"
  sh "erlc  #{ERLC_FLAGS} #{ENV['TEST'] ? ERLC_TEST_FLAGS : ''} elibs/*.erl gen-erl/*.erl"
  # Dir["templates/*"].each do |template|
  #   sh %Q(erl -pz ebin -noshell -eval 'erltl:compile("#{template}", [{outdir, "ebin"}, debug_info, show_errors, show_warnings])' -s erlang halt)
  # end
end

task :test_env => [:build_test_deps] do
  puts "test env"
  ENV['TEST'] = 'test'
end

task :native do
  puts "not implemented"
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
  # -run #{ENV['MOD']} test
  sh %Q{erl -boot start_sasl +K true -smp enable -pz ./etest -pz ./ebin/yaws -pz ./ebin/ -pa ./deps/eunit/ebin -pa deps/mochiweb/ebin -pa deps/rfc4627/ebin -sname local_console_#{$$}  -noshell -s eunit test #{mod_directives} -run init stop}
end

task :docs do
  #files = (Dir["elibs/*.erl"] - ["elibs/json.erl"]).sort.map { |x| "\'../" + x + "\'"}.join(" ")
  #sh %|cd doc && erl -noshell -run edoc_run files #{files}|
  files = Dir["elibs/*.erl"].map { |x| "'../" + x + "'"}.join " "
  sh %|cd doc && erl -noshell -s init stop -run edoc files #{files}|
end

task :build_deps do
  Dir["deps/*"].each do |dir|
    sh "cd #{dir} && make"
  end
end

task :build_test_deps do
  sh "erlc  #{ERLC_FLAGS} #{ERLC_TEST_FLAGS} etest/t.erl"
end
