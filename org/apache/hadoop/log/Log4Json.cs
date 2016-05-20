using Sharpen;

namespace org.apache.hadoop.log
{
	/// <summary>This offers a log layout for JSON, with some test entry points.</summary>
	/// <remarks>
	/// This offers a log layout for JSON, with some test entry points. It's purpose is
	/// to allow Log4J to generate events that are easy for other programs to parse, but which are somewhat
	/// human-readable.
	/// Some features.
	/// <ol>
	/// <li>Every event is a standalone JSON clause</li>
	/// <li>Time is published as a time_t event since 1/1/1970
	/// -this is the fastest to generate.</li>
	/// <li>An ISO date is generated, but this is cached and will only be accurate to within a second</li>
	/// <li>the stack trace is included as an array</li>
	/// </ol>
	/// A simple log event will resemble the following
	/// <pre>
	/// {"name":"test","time":1318429136789,"date":"2011-10-12 15:18:56,789","level":"INFO","thread":"main","message":"test message"}
	/// </pre>
	/// An event with an error will contain data similar to that below (which has been reformatted to be multi-line).
	/// <pre>
	/// {
	/// "name":"testException",
	/// "time":1318429136789,
	/// "date":"2011-10-12 15:18:56,789",
	/// "level":"INFO",
	/// "thread":"quoted\"",
	/// "message":"new line\n and {}",
	/// "exceptionclass":"java.net.NoRouteToHostException",
	/// "stack":[
	/// "java.net.NoRouteToHostException: that box caught fire 3 years ago",
	/// "\tat org.apache.hadoop.log.TestLog4Json.testException(TestLog4Json.java:49)",
	/// "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
	/// "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)",
	/// "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)",
	/// "\tat java.lang.reflect.Method.invoke(Method.java:597)",
	/// "\tat junit.framework.TestCase.runTest(TestCase.java:168)",
	/// "\tat junit.framework.TestCase.runBare(TestCase.java:134)",
	/// "\tat junit.framework.TestResult$1.protect(TestResult.java:110)",
	/// "\tat junit.framework.TestResult.runProtected(TestResult.java:128)",
	/// "\tat junit.framework.TestResult.run(TestResult.java:113)",
	/// "\tat junit.framework.TestCase.run(TestCase.java:124)",
	/// "\tat junit.framework.TestSuite.runTest(TestSuite.java:232)",
	/// "\tat junit.framework.TestSuite.run(TestSuite.java:227)",
	/// "\tat org.junit.internal.runners.JUnit38ClassRunner.run(JUnit38ClassRunner.java:83)",
	/// "\tat org.apache.maven.surefire.junit4.JUnit4TestSet.execute(JUnit4TestSet.java:59)",
	/// "\tat org.apache.maven.surefire.suite.AbstractDirectoryTestSuite.executeTestSet(AbstractDirectoryTestSuite.java:120)",
	/// "\tat org.apache.maven.surefire.suite.AbstractDirectoryTestSuite.execute(AbstractDirectoryTestSuite.java:145)",
	/// "\tat org.apache.maven.surefire.Surefire.run(Surefire.java:104)",
	/// "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
	/// "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)",
	/// "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)",
	/// "\tat java.lang.reflect.Method.invoke(Method.java:597)",
	/// "\tat org.apache.maven.surefire.booter.SurefireBooter.runSuitesInProcess(SurefireBooter.java:290)",
	/// "\tat org.apache.maven.surefire.booter.SurefireBooter.main(SurefireBooter.java:1017)"
	/// ]
	/// }
	/// </pre>
	/// </remarks>
	public class Log4Json : org.apache.log4j.Layout
	{
		/// <summary>Jackson factories are thread safe when constructing parsers and generators.
		/// 	</summary>
		/// <remarks>
		/// Jackson factories are thread safe when constructing parsers and generators.
		/// They are not thread safe in configure methods; if there is to be any
		/// configuration it must be done in a static intializer block.
		/// </remarks>
		private static readonly org.codehaus.jackson.JsonFactory factory = new org.codehaus.jackson.map.MappingJsonFactory
			();

		public const string DATE = "date";

		public const string EXCEPTION_CLASS = "exceptionclass";

		public const string LEVEL = "level";

		public const string MESSAGE = "message";

		public const string NAME = "name";

		public const string STACK = "stack";

		public const string THREAD = "thread";

		public const string TIME = "time";

		public const string JSON_TYPE = "application/json";

		private readonly java.text.DateFormat dateFormat;

		public Log4Json()
		{
			dateFormat = new org.apache.log4j.helpers.ISO8601DateFormat();
		}

		/// <returns>the mime type of JSON</returns>
		public override string getContentType()
		{
			return JSON_TYPE;
		}

		public override string format(org.apache.log4j.spi.LoggingEvent @event)
		{
			try
			{
				return toJson(@event);
			}
			catch (System.IO.IOException e)
			{
				//this really should not happen, and rather than throw an exception
				//which may hide the real problem, the log class is printed
				//in JSON format. The classname is used to ensure valid JSON is 
				//returned without playing escaping games
				return "{ \"logfailure\":\"" + Sharpen.Runtime.getClassForObject(e).ToString() + 
					"\"}";
			}
		}

		/// <summary>Convert an event to JSON</summary>
		/// <param name="event">the event -must not be null</param>
		/// <returns>a string value</returns>
		/// <exception cref="System.IO.IOException">on problems generating the JSON</exception>
		public virtual string toJson(org.apache.log4j.spi.LoggingEvent @event)
		{
			System.IO.StringWriter writer = new System.IO.StringWriter();
			toJson(writer, @event);
			return writer.ToString();
		}

		/// <summary>Convert an event to JSON</summary>
		/// <param name="writer">the destination writer</param>
		/// <param name="event">the event -must not be null</param>
		/// <returns>the writer</returns>
		/// <exception cref="System.IO.IOException">on problems generating the JSON</exception>
		public virtual System.IO.TextWriter toJson(System.IO.TextWriter writer, org.apache.log4j.spi.LoggingEvent
			 @event)
		{
			org.apache.log4j.spi.ThrowableInformation ti = @event.getThrowableInformation();
			toJson(writer, @event.getLoggerName(), @event.getTimeStamp(), @event.getLevel().ToString
				(), @event.getThreadName(), @event.getRenderedMessage(), ti);
			return writer;
		}

		/// <summary>Build a JSON entry from the parameters.</summary>
		/// <remarks>Build a JSON entry from the parameters. This is public for testing.</remarks>
		/// <param name="writer">destination</param>
		/// <param name="loggerName">logger name</param>
		/// <param name="timeStamp">time_t value</param>
		/// <param name="level">level string</param>
		/// <param name="threadName">name of the thread</param>
		/// <param name="message">rendered message</param>
		/// <param name="ti">nullable thrown information</param>
		/// <returns>the writer</returns>
		/// <exception cref="System.IO.IOException">on any problem</exception>
		public virtual System.IO.TextWriter toJson(System.IO.TextWriter writer, string loggerName
			, long timeStamp, string level, string threadName, string message, org.apache.log4j.spi.ThrowableInformation
			 ti)
		{
			org.codehaus.jackson.JsonGenerator json = factory.createJsonGenerator(writer);
			json.writeStartObject();
			json.writeStringField(NAME, loggerName);
			json.writeNumberField(TIME, timeStamp);
			System.DateTime date = new System.DateTime(timeStamp);
			json.writeStringField(DATE, dateFormat.format(date));
			json.writeStringField(LEVEL, level);
			json.writeStringField(THREAD, threadName);
			json.writeStringField(MESSAGE, message);
			if (ti != null)
			{
				//there is some throwable info, but if the log event has been sent over the wire,
				//there may not be a throwable inside it, just a summary.
				System.Exception thrown = ti.getThrowable();
				string eclass = (thrown != null) ? Sharpen.Runtime.getClassForObject(thrown).getName
					() : string.Empty;
				json.writeStringField(EXCEPTION_CLASS, eclass);
				string[] stackTrace = ti.getThrowableStrRep();
				json.writeArrayFieldStart(STACK);
				foreach (string row in stackTrace)
				{
					json.writeString(row);
				}
				json.writeEndArray();
			}
			json.writeEndObject();
			json.flush();
			json.close();
			return writer;
		}

		/// <summary>This appender does not ignore throwables</summary>
		/// <returns>false, always</returns>
		public override bool ignoresThrowable()
		{
			return false;
		}

		/// <summary>Do nothing</summary>
		public override void activateOptions()
		{
		}

		/// <summary>For use in tests</summary>
		/// <param name="json">incoming JSON to parse</param>
		/// <returns>a node tree</returns>
		/// <exception cref="System.IO.IOException">on any parsing problems</exception>
		public static org.codehaus.jackson.node.ContainerNode parse(string json)
		{
			org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper
				(factory);
			org.codehaus.jackson.JsonNode jsonNode = mapper.readTree(json);
			if (!(jsonNode is org.codehaus.jackson.node.ContainerNode))
			{
				throw new System.IO.IOException("Wrong JSON data: " + json);
			}
			return (org.codehaus.jackson.node.ContainerNode)jsonNode;
		}
	}
}
