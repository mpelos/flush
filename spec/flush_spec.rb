require 'spec_helper'

describe Flush do
  describe ".flushfile" do
    let(:path) { Pathname("/tmp/Flushfile.rb") }

    context "Flushfile.rb is missing from pwd" do
      it "raises an exception" do
        path.delete if path.exist?
        Flush.configuration.flushfile = path

        expect { Flush.flushfile }.to raise_error(Errno::ENOENT)
      end
    end

    context "Flushfile.rb exists" do
      it "returns Pathname to it" do
        FileUtils.touch(path)
        Flush.configuration.flushfile = path
        expect(Flush.flushfile).to eq(path.realpath)
        path.delete
      end
    end
  end

  describe ".root" do
    it "returns root directory of Flush" do
      expected = Pathname.new(__FILE__).parent.parent
      expect(Flush.root).to eq(expected)
    end
  end

  describe ".configure" do
    it "runs block with config instance passed" do
      expect { |b| Flush.configure(&b) }.to yield_with_args(Flush.configuration)
    end
  end

end
