module JRubyOnHadoop
  JAVA_MAIN_CLASS = 'org.apache.hadoop.ruby.JRubyJobRunner' 

  class Client
    attr_reader :script, :inputs, :outputs, :files

    def initialize(args=[])
      @args = args
      parse_args

      # env get / set and check
      hadoop_home and hadoop_cmd and hadoop_classpath
    end

    def hadoop_home
      ENV['HADOOP_HOME']
    end

    def hadoop_cmd
      hadoop = `which hadoop 2>/dev/null`
      hadoop = "#{hadoop_home}/bin/hadoop" if hadoop.empty? and (!hadoop_home.empty?)
      raise 'cannot find hadoop command' if hadoop.empty?
      hadoop.chomp
    end

    def hadoop_classpath
      require 'pp'
      ENV['HADOOP_CLASSPATH'] ||= ""
      ENV['HADOOP_CLASSPATH'] += ([lib_path] + @dirnames + jruby_jars).join(':')
    end

    def run
      puts cmd if ENV["VERBOSE"]
      exec cmd
    end

    def cmd
      "#{hadoop_cmd} jar #{main_jar_path} #{JAVA_MAIN_CLASS}" +
      " -libjars #{opt_libjars} -files #{opt_files} #{mapred_args}"
    end

    def parse_args
      raise "Usage: joh script_path input_path output_path" if @args.size < 3
      @script_path = @args[0]
      @script = File.basename(@script_path)
      @files = [@script_path, JRubyOnHadoop.wrapper_ruby_file]
      @files << ENV["HADOOP_FILES"] if ENV["HADOOP_FILES"]
      @dirnames = [File.dirname(@script_path), File.dirname(JRubyOnHadoop.wrapper_ruby_file)]
      @args.each do |arg|
        if File.file?(arg) and !@files.include?(arg) 
          @files << arg
          @dirnames << File.dirname(arg)
        end
      end
    end

    def mapred_args
      args = ""
      args += ENV['HADOOP_ARGS'] + " " if ENV['HADOOP_ARGS']
      args += "--script #{@script} "
      (1..@args.size-1).each do |index| 
        arg = @args[index]
        # arg = File.basename(arg) if File.file?(arg)
        # arg = File.basename(arg) if File.file?(arg)
        args += "#{arg} "
      end
      args
    end

    def jruby_jars
      [JRubyJars.core_jar_path, JRubyJars.stdlib_jar_path]
    end

    def opt_libjars; jruby_jars.join(',') end
    def opt_files; @files.join(',') end
    def main_jar_path; JRubyOnHadoop.jar_path end
    def lib_path; JRubyOnHadoop.lib_path end
  end
end
