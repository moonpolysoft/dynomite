# Common build system
require 'rubygems'
require 'rake'

ERLC_TEST_FLAGS = "-pa deps/eunit/ebin -I deps/eunit/include -DTEST"
ERLC_FLAGS = "+debug_info -W0 -I include -pa deps/mochiweb/ebin -I deps/mochiweb/include -pa deps/rfc4627/ebin -I deps/rfc4627/include -o ebin"

task :default => [:build_deps] do
  sh "erlc  #{ERLC_FLAGS} #{ENV['TEST'] ? ERLC_TEST_FLAGS : ''} elibs/*.erl"
  # Dir["templates/*"].each do |template|
  #   sh %Q(erl -pz ebin -noshell -eval 'erltl:compile("#{template}", [{outdir, "ebin"}, debug_info, show_errors, show_warnings])' -s erlang halt)
  # end
end

task :run do
  sh %Q{erl -boot start_sasl +K true -smp enable -pz ./ebin/ -sname local_console#{$$} -mnesia dir '"/tmp/mbd"' -noshell -run dynomite start}
end

task :build_dist => [:build_deps] do
  sh "erlc #{ERLC_FLAGS} elibs/*.erl"
  # Dir["templates/*"].each do |template|
  #   sh %Q(erl -pz ebin -noshell -eval 'erltl:compile("#{template}", [{outdir, "ebin"}, debug_info, show_errors, show_warnings])' -s erlang halt)
  # end
end

task :econsole do
  sh "erl +Bc +K true -smp enable -pz ./ebin -pz ./etest -pa ./deps/eunit/ebin -pa deps/rfc4627/ebin -pa deps/mochiweb/ebin -sname local_console_#{$$} -kernel start_boot_server true"
end

task :console do
  sh "irb -I rlibs/"
end

task :test => [:default] do
  mods = []
  mod_directives = ""
  env_peek = ENV['MOD'] || ENV['MODS'] || ENV['MODULE'] || ENV['MODULES']
  if env_peek
    mods = env_peek.split(",")
  else 
    mods = Dir["etest/*_test.erl"].map { |x| x.match(/etest\/(.*)_test.erl/)[1] }
  end
  mod_directives = mods.map {|m| "-run #{m} test"}.join(" ")
  # -run #{ENV['MOD']} test
  sh %Q{erl -boot start_sasl +K true -smp enable -pz ./etest -pz ./ebin/yaws -pz ./ebin/ -pa ./deps/eunit/ebin -pa deps/mochiweb/ebin -pa deps/rfc4627/ebin -sname local_console_#{$$} -mnesia dir '"/tmp/mdb"' -noshell #{mod_directives} -run erlang halt}
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