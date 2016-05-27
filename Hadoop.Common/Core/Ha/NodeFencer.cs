using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.HA
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
		private const string ClassRe = "([a-zA-Z0-9\\.\\$]+)";

		private static readonly Sharpen.Pattern ClassWithArgument = Sharpen.Pattern.Compile
			(ClassRe + "\\((.+?)\\)");

		private static readonly Sharpen.Pattern ClassWithoutArgument = Sharpen.Pattern.Compile
			(ClassRe);

		private static readonly Sharpen.Pattern HashCommentRe = Sharpen.Pattern.Compile("#.*$"
			);

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.NodeFencer
			));

		/// <summary>Standard fencing methods included with Hadoop.</summary>
		private static readonly IDictionary<string, Type> StandardMethods = ImmutableMap.
			Of<string, Type>("shell", typeof(ShellCommandFencer), "sshfence", typeof(SshFenceByTcpPort
			));

		private readonly IList<NodeFencer.FenceMethodWithArg> methods;

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		internal NodeFencer(Configuration conf, string spec)
		{
			this.methods = ParseMethods(conf, spec);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public static Org.Apache.Hadoop.HA.NodeFencer Create(Configuration conf, string confKey
			)
		{
			string confStr = conf.Get(confKey);
			if (confStr == null)
			{
				return null;
			}
			return new Org.Apache.Hadoop.HA.NodeFencer(conf, confStr);
		}

		public virtual bool Fence(HAServiceTarget fromSvc)
		{
			Log.Info("====== Beginning Service Fencing Process... ======");
			int i = 0;
			foreach (NodeFencer.FenceMethodWithArg method in methods)
			{
				Log.Info("Trying method " + (++i) + "/" + methods.Count + ": " + method);
				try
				{
					if (method.method.TryFence(fromSvc, method.arg))
					{
						Log.Info("====== Fencing successful by method " + method + " ======");
						return true;
					}
				}
				catch (BadFencingConfigurationException e)
				{
					Log.Error("Fencing method " + method + " misconfigured", e);
					continue;
				}
				catch (Exception t)
				{
					Log.Error("Fencing method " + method + " failed with an unexpected error.", t);
					continue;
				}
				Log.Warn("Fencing method " + method + " was unsuccessful.");
			}
			Log.Error("Unable to fence service by any configured method.");
			return false;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		private static IList<NodeFencer.FenceMethodWithArg> ParseMethods(Configuration conf
			, string spec)
		{
			string[] lines = spec.Split("\\s*\n\\s*");
			IList<NodeFencer.FenceMethodWithArg> methods = Lists.NewArrayList();
			foreach (string line in lines)
			{
				line = HashCommentRe.Matcher(line).ReplaceAll(string.Empty);
				line = line.Trim();
				if (!line.IsEmpty())
				{
					methods.AddItem(ParseMethod(conf, line));
				}
			}
			return methods;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		private static NodeFencer.FenceMethodWithArg ParseMethod(Configuration conf, string
			 line)
		{
			Matcher m;
			if ((m = ClassWithArgument.Matcher(line)).Matches())
			{
				string className = m.Group(1);
				string arg = m.Group(2);
				return CreateFenceMethod(conf, className, arg);
			}
			else
			{
				if ((m = ClassWithoutArgument.Matcher(line)).Matches())
				{
					string className = m.Group(1);
					return CreateFenceMethod(conf, className, null);
				}
				else
				{
					throw new BadFencingConfigurationException("Unable to parse line: '" + line + "'"
						);
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		private static NodeFencer.FenceMethodWithArg CreateFenceMethod(Configuration conf
			, string clazzName, string arg)
		{
			Type clazz;
			try
			{
				// See if it's a short name for one of the built-in methods
				clazz = StandardMethods[clazzName];
				if (clazz == null)
				{
					// Try to instantiate the user's custom method
					clazz = Sharpen.Runtime.GetType(clazzName);
				}
			}
			catch (Exception e)
			{
				throw new BadFencingConfigurationException("Could not find configured fencing method "
					 + clazzName, e);
			}
			// Check that it implements the right interface
			if (!typeof(FenceMethod).IsAssignableFrom(clazz))
			{
				throw new BadFencingConfigurationException("Class " + clazzName + " does not implement FenceMethod"
					);
			}
			FenceMethod method = (FenceMethod)ReflectionUtils.NewInstance(clazz, conf);
			method.CheckArgs(arg);
			return new NodeFencer.FenceMethodWithArg(method, arg);
		}

		private class FenceMethodWithArg
		{
			private readonly FenceMethod method;

			private readonly string arg;

			private FenceMethodWithArg(FenceMethod method, string arg)
			{
				this.method = method;
				this.arg = arg;
			}

			public override string ToString()
			{
				return method.GetType().GetCanonicalName() + "(" + arg + ")";
			}
		}
	}
}
