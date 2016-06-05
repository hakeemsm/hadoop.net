using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class TestResourceUsage
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.TestResourceUsage
			));

		private string suffix;

		[Parameterized.Parameters]
		public static ICollection<string[]> GetParameters()
		{
			return Arrays.AsList(new string[][] { new string[] { "Pending" }, new string[] { 
				"Used" }, new string[] { "Reserved" }, new string[] { "AMUsed" } });
		}

		public TestResourceUsage(string suffix)
		{
			this.suffix = suffix;
		}

		/// <exception cref="System.Exception"/>
		private static void Dec(ResourceUsage obj, string suffix, Resource res, string label
			)
		{
			ExecuteByName(obj, "dec" + suffix, res, label);
		}

		/// <exception cref="System.Exception"/>
		private static void Inc(ResourceUsage obj, string suffix, Resource res, string label
			)
		{
			ExecuteByName(obj, "inc" + suffix, res, label);
		}

		/// <exception cref="System.Exception"/>
		private static void Set(ResourceUsage obj, string suffix, Resource res, string label
			)
		{
			ExecuteByName(obj, "set" + suffix, res, label);
		}

		/// <exception cref="System.Exception"/>
		private static Resource Get(ResourceUsage obj, string suffix, string label)
		{
			return ExecuteByName(obj, "get" + suffix, null, label);
		}

		// Use reflection to avoid too much avoid code
		/// <exception cref="System.Exception"/>
		private static Resource ExecuteByName(ResourceUsage obj, string methodName, Resource
			 arg, string label)
		{
			// We have 4 kinds of method
			// 1. getXXX() : Resource
			// 2. getXXX(label) : Resource
			// 3. set/inc/decXXX(res) : void
			// 4. set/inc/decXXX(label, res) : void
			if (methodName.StartsWith("get"))
			{
				Resource result;
				if (label == null)
				{
					// 1.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(ResourceUsage), methodName
						);
					result = (Resource)method.Invoke(obj);
				}
				else
				{
					// 2.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(ResourceUsage), methodName
						, typeof(string));
					result = (Resource)method.Invoke(obj, label);
				}
				return result;
			}
			else
			{
				if (label == null)
				{
					// 3.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(ResourceUsage), methodName
						, typeof(Resource));
					method.Invoke(obj, arg);
				}
				else
				{
					// 4.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(ResourceUsage), methodName
						, typeof(string), typeof(Resource));
					method.Invoke(obj, label, arg);
				}
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void InternalTestModifyAndRead(string label)
		{
			ResourceUsage usage = new ResourceUsage();
			Resource res;
			// First get returns 0 always
			res = Get(usage, suffix, label);
			Check(0, 0, res);
			// Add 1,1 should returns 1,1
			Inc(usage, suffix, Resource.NewInstance(1, 1), label);
			Check(1, 1, Get(usage, suffix, label));
			// Set 2,2
			Set(usage, suffix, Resource.NewInstance(2, 2), label);
			Check(2, 2, Get(usage, suffix, label));
			// dec 2,2
			Dec(usage, suffix, Resource.NewInstance(2, 2), label);
			Check(0, 0, Get(usage, suffix, label));
		}

		internal virtual void Check(int mem, int cpu, Resource res)
		{
			NUnit.Framework.Assert.AreEqual(mem, res.GetMemory());
			NUnit.Framework.Assert.AreEqual(cpu, res.GetVirtualCores());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAndRead()
		{
			Log.Info("Test - " + suffix);
			InternalTestModifyAndRead(null);
			InternalTestModifyAndRead("label");
		}
	}
}
