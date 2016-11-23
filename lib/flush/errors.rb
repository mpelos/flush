module Flush
  class WorkflowNotFound < StandardError; end
  class DependencyLevelTooDeep < StandardError; end
  class MissingRequiredJobArguments < StandardError; end
  class AttributeNotAPromise < StandardError; end
  class JobWorkflowNotFoundInDescendants < StandardError; end
  class ExposeParameterNotFoundInOutput < StandardError; end
  class AttributeNotResolved < StandardError; end
end
