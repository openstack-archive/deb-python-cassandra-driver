# Lovingly stolen from https://github.com/realm/jazzy/blob/master/Dangerfile

has_app_changes = !git.modified_files.grep(/cassandra/).empty?
if !git.modified_files.include?("CHANGELOG.rst") && has_app_changes
  fail "You've modified application code. " +
    "Unless this is strictly a refactoring, please include a CHANGELOG entry."
end
