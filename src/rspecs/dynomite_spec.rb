require File.dirname(__FILE__) + "/spec_helper"

describe Dynomite do
  it "should connect and disconnect cleanly" do
    lambda do
      dyn = Dynomite.new(:port => 11211, :host => 'localhost')
      dyn.close
    end.should_not raise_error
  end
  
  it "should execute clean put operations cleanly" do
    dyn = Dynomite.new(:port => 11211, :host => 'localhost')
    dyn.put("mahkey", nil, "mahvalue").should == 1
    dyn.close
  end
end