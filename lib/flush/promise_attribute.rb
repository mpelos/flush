module Flush
  class PromiseAttribute
    attr_reader :attr_name

    def initialize(attr_name)
      @attr_name = attr_name
    end

    def method_missing(method_name)
      fail AttributeNotResolved, "This attribute is just a reference and can't call this method; " +
       "attribute: #{attr_name}, called_method: #{method_name}"
    end
  end
end
