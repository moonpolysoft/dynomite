require File.dirname(__FILE__) + "/spec_helper"

describe Dynomite do
  before(:each) do
    @dyn = Dynomite.new
    @write = StringIO.new("", "w")
    @read = StringIO.new("", "r")
    write = @write
    read = @read
    @dyn.meta_def :write do |data|
      write.write(data)
    end
    @dyn.meta_def :read do |length|
      read.read(length)
    end
  end
  
  it "should execute clean put operations" do
    @read.string = "succ 1\n"
    @dyn.put("mahkey", nil, "mahvalue").should == 1
    @write.string.should == "put 6 mahkey 0  8 mahvalue\n"
  end
  
  it "should execute context put operations" do
    @read.string = "succ 1\n"
    @dyn.put("mahkey", "mycontext", "mahvalue").should == 1
    @write.string.should == "put 6 mahkey 9 mycontext 8 mahvalue\n"
  end
  
  it "should execute get operations" do
    @read.string = "succ 2 9 mycontext 8 mahvalue 7 myvalue\n"
    @dyn.get("mahkey").should == ["mycontext", ["mahvalue", "myvalue"]]
    @write.string.should == "get 6 mahkey\n"
  end
  
  it "should execute true has key operations" do
    @read.string = "yes 3\n"
    @dyn.has_key("mahkey").should == [true, 3]
    @write.string.should == "has 6 mahkey\n"
  end
  
  it "should execute false has key operations" do
    @read.string = "no 3\n"
    @dyn.has_key("mahkey").should == [false,3]
    @write.string.should == "has 6 mahkey\n"
  end
  
  it "should execute delete operations" do
    @read.string = "succ 3\n"
    @dyn.delete("mahkey").should == 3
    @write.string.should == "del 6 mahkey\n"
  end
  
  it "should throw an error for bad put operations" do
    @read.string = "fail avast ye mateys\n"
    lambda { @dyn.put("mahkey", "mycontext", "mahvalue") }.should raise_error(DynomiteError, "avast ye mateys")
  end
  
  it "should throw an error for bad get operations" do
    @read.string = "fail monkey wrench\n"
    lambda { @dyn.get("mahkey") }.should raise_error(DynomiteError, "monkey wrench")
  end
  
  it "should throw an error for bad has_key operations" do
    @read.string = "fail argg me harty\n"
    lambda { @dyn.get("mahkey") }.should raise_error(DynomiteError, "argg me harty")
  end
  
  it "should throw an error for bad delete operations" do
    @read.string = "fail ohhh god no\n"
    lambda { @dyn.get("mahkey") }.should raise_error(DynomiteError, "ohhh god no")
  end
end