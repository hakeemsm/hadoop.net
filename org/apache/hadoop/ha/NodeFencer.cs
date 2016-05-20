using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// This class parses the configured list of fencing methods, and
	/// is responsible for trying each one in turn while logging informative
	/// output.<p>
	/// The fencing methods are configured as a carriage-return separated list.
	/// </summary>
	/// <remarks>
	/// This class parses the configured list of fencing methods, and
	/// is responsible for trying each one in turn while logging informative
	/// output.<p>
	/// The fencing methods are configured as a carriage-return separated list.
	/// Each line in the list is of the form:<p>
	/// <code>com.example.foo.MyMethod(arg string)</code>
	/// or
	/// <code>com.example.foo.MyMethod</code>
	/// The class provided must implement the
	/// <see cref="FenceMethod"/>
	/// interface.
	/// The fencing methods that ship with Hadoop may also be referred to
	/// by shortened names:<p>
	/// <ul>
	/// <li><code>shell(/path/to/some/script.sh args...)</code></li>
	/// <li><code>sshfence(...)</code> (see
	/// <see cref="SshFenceByTcpPort"/>
	/// )
	/// </ul>
	/// </remarks>
	public class NodeFencer
	{
		private const string CLASS_RE = "([a-zA-Z0-9\\.\\$]+)";

		private static readonly java.util.regex.Pattern CLASS_WITH_ARGUMENT = java.util.regex.Pattern
			.compile(CLASS_RE + "\\((.+?)\\)");

		private static readonly java.util.regex.Pattern CLASS_WITHOUT_ARGUMENT = java.util.regex.Pattern
			.compile(CLASS_RE);

		private static readonly java.util.regex.Pattern HASH_COMMENT_RE = java.util.regex.Pattern
			.compile("#.*$");

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.NodeFencer))
			);

		/// <summary>Standard fencing methods included with Hadoop.</summary>
		private static readonly System.Collections.Generic.IDictionary<string, java.lang.Class
			> STANDARD_METHODS = com.google.common.collect.ImmutableMap.of<string, java.lang.Class
			>("shell", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ShellCommandFencer
			)), "sshfence", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.SshFenceByTcpPort
			)));

		private readonly System.Collections.Generic.IList<org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg
			> methods;

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		internal NodeFencer(org.apache.hadoop.conf.Configuration conf, string spec)
		{
			this.methods = parseMethods(conf, spec);
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public static org.apache.hadoop.ha.NodeFencer create(org.apache.hadoop.conf.Configuration
			 conf, string confKey)
		{
			string confStr = conf.get(confKey);
			if (confStr == null)
			{
				return null;
			}
			return new org.apache.hadoop.ha.NodeFencer(conf, confStr);
		}

		public virtual bool fence(org.apache.hadoop.ha.HAServiceTarget fromSvc)
		{
			LOG.info("====== Beginning Service Fencing Process... ======");
			int i = 0;
			foreach (org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg method in methods)
			{
				LOG.info("Trying method " + (++i) + "/" + methods.Count + ": " + method);
				try
				{
					if (method.method.tryFence(fromSvc, method.arg))
					{
						LOG.info("====== Fencing successful by method " + method + " ======");
						return true;
					}
				}
				catch (org.apache.hadoop.ha.BadFencingConfigurationException e)
				{
					LOG.error("Fencing method " + method + " misconfigured", e);
					continue;
				}
				catch (System.Exception t)
				{
					LOG.error("Fencing method " + method + " failed with an unexpected error.", t);
					continue;
				}
				LOG.warn("Fencing method " + method + " was unsuccessful.");
			}
			LOG.error("Unable to fence service by any configured method.");
			return false;
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		private static System.Collections.Generic.IList<org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg
			> parseMethods(org.apache.hadoop.conf.Configuration conf, string spec)
		{
			string[] lines = spec.split("\\s*\n\\s*");
			System.Collections.Generic.IList<org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg
				> methods = com.google.common.collect.Lists.newArrayList();
			foreach (string line in lines)
			{
				line = HASH_COMMENT_RE.matcher(line).replaceAll(string.Empty);
				line = line.Trim();
				if (!line.isEmpty())
				{
					methods.add(parseMethod(conf, line));
				}
			}
			return methods;
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		private static org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg parseMethod(org.apache.hadoop.conf.Configuration
			 conf, string line)
		{
			java.util.regex.Matcher m;
			if ((m = CLASS_WITH_ARGUMENT.matcher(line)).matches())
			{
				string className = m.group(1);
				string arg = m.group(2);
				return createFenceMethod(conf, className, arg);
			}
			else
			{
				if ((m = CLASS_WITHOUT_ARGUMENT.matcher(line)).matches())
				{
					string className = m.group(1);
					return createFenceMethod(conf, className, null);
				}
				else
				{
					throw new org.apache.hadoop.ha.BadFencingConfigurationException("Unable to parse line: '"
						 + line + "'");
				}
			}
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		private static org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg createFenceMethod
			(org.apache.hadoop.conf.Configuration conf, string clazzName, string arg)
		{
			java.lang.Class clazz;
			try
			{
				// See if it's a short name for one of the built-in methods
				clazz = STANDARD_METHODS[clazzName];
				if (clazz == null)
				{
					// Try to instantiate the user's custom method
					clazz = java.lang.Class.forName(clazzName);
				}
			}
			catch (System.Exception e)
			{
				throw new org.apache.hadoop.ha.BadFencingConfigurationException("Could not find configured fencing method "
					 + clazzName, e);
			}
			// Check that it implements the right interface
			if (!Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.FenceMethod)).isAssignableFrom
				(clazz))
			{
				throw new org.apache.hadoop.ha.BadFencingConfigurationException("Class " + clazzName
					 + " does not implement FenceMethod");
			}
			org.apache.hadoop.ha.FenceMethod method = (org.apache.hadoop.ha.FenceMethod)org.apache.hadoop.util.ReflectionUtils
				.newInstance(clazz, conf);
			method.checkArgs(arg);
			return new org.apache.hadoop.ha.NodeFencer.FenceMethodWithArg(method, arg);
		}

		private class FenceMethodWithArg
		{
			private readonly org.apache.hadoop.ha.FenceMethod method;

			private readonly string arg;

			private FenceMethodWithArg(org.apache.hadoop.ha.FenceMethod method, string arg)
			{
				this.method = method;
				this.arg = arg;
			}

			public override string ToString()
			{
				return Sharpen.Runtime.getClassForObject(method).getCanonicalName() + "(" + arg +
					 ")";
			}
		}
	}
}
