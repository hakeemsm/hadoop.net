using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A tool interface that supports handling of generic command-line options.
	/// 	</summary>
	/// <remarks>
	/// A tool interface that supports handling of generic command-line options.
	/// <p><code>Tool</code>, is the standard for any Map-Reduce tool/application.
	/// The tool/application should delegate the handling of
	/// &lt;a href="
	/// <docRoot/>
	/// /../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options"&gt;
	/// standard command-line options</a> to
	/// <see cref="ToolRunner.Run(Tool, string[])"/>
	/// 
	/// and only handle its custom arguments.</p>
	/// <p>Here is how a typical <code>Tool</code> is implemented:</p>
	/// <p><blockquote><pre>
	/// public class MyApp extends Configured implements Tool {
	/// public int run(String[] args) throws Exception {
	/// // <code>Configuration</code> processed by <code>ToolRunner</code>
	/// Configuration conf = getConf();
	/// // Create a JobConf using the processed <code>conf</code>
	/// JobConf job = new JobConf(conf, MyApp.class);
	/// // Process custom command-line options
	/// Path in = new Path(args[1]);
	/// Path out = new Path(args[2]);
	/// // Specify various job-specific parameters
	/// job.setJobName("my-app");
	/// job.setInputPath(in);
	/// job.setOutputPath(out);
	/// job.setMapperClass(MyMapper.class);
	/// job.setReducerClass(MyReducer.class);
	/// // Submit the job, then poll for progress until the job is complete
	/// JobClient.runJob(job);
	/// return 0;
	/// }
	/// public static void main(String[] args) throws Exception {
	/// // Let <code>ToolRunner</code> handle generic command-line options
	/// int res = ToolRunner.run(new Configuration(), new MyApp(), args);
	/// System.exit(res);
	/// }
	/// }
	/// </pre></blockquote></p>
	/// </remarks>
	/// <seealso cref="GenericOptionsParser"/>
	/// <seealso cref="ToolRunner"/>
	public interface Tool : Configurable
	{
		/// <summary>Execute the command with the given arguments.</summary>
		/// <param name="args">command specific arguments.</param>
		/// <returns>exit code.</returns>
		/// <exception cref="System.Exception"/>
		int Run(string[] args);
	}
}
