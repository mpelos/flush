module Flush
  class WorkflowNotFound < StandardError; end
  class DependencyLevelTooDeep < StandardError; end
end
