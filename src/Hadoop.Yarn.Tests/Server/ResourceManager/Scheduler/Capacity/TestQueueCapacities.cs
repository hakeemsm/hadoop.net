using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestQueueCapacities
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.TestQueueCapacities
			));

		private string suffix;

		[Parameterized.Parameters]
		public static ICollection<string[]> GetParameters()
		{
			return Arrays.AsList(new string[][] { new string[] { "Capacity" }, new string[] { 
				"AbsoluteCapacity" }, new string[] { "UsedCapacity" }, new string[] { "AbsoluteUsedCapacity"
				 }, new string[] { "MaximumCapacity" }, new string[] { "AbsoluteMaximumCapacity"
				 } });
		}

		public TestQueueCapacities(string suffix)
		{
			this.suffix = suffix;
		}

		/// <exception cref="System.Exception"/>
		private static float Get(QueueCapacities obj, string suffix, string label)
		{
			return ExecuteByName(obj, "get" + suffix, label, -1f);
		}

		/// <exception cref="System.Exception"/>
		private static void Set(QueueCapacities obj, string suffix, string label, float value
			)
		{
			ExecuteByName(obj, "set" + suffix, label, value);
		}

		// Use reflection to avoid too much avoid code
		/// <exception cref="System.Exception"/>
		private static float ExecuteByName(QueueCapacities obj, string methodName, string
			 label, float value)
		{
			// We have 4 kinds of method
			// 1. getXXX() : float
			// 2. getXXX(label) : float
			// 3. setXXX(float) : void
			// 4. setXXX(label, float) : void
			if (methodName.StartsWith("get"))
			{
				float result;
				if (label == null)
				{
					// 1.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(QueueCapacities), methodName
						);
					result = (float)method.Invoke(obj);
				}
				else
				{
					// 2.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(QueueCapacities), methodName
						, typeof(string));
					result = (float)method.Invoke(obj, label);
				}
				return result;
			}
			else
			{
				if (label == null)
				{
					// 3.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(QueueCapacities), methodName
						, typeof(float));
					method.Invoke(obj, value);
				}
				else
				{
					// 4.
					MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(typeof(QueueCapacities), methodName
						, typeof(string), typeof(float));
					method.Invoke(obj, label, value);
				}
				return -1f;
			}
		}

		/// <exception cref="System.Exception"/>
		private void InternalTestModifyAndRead(string label)
		{
			QueueCapacities qc = new QueueCapacities(false);
			// First get returns 0 always
			NUnit.Framework.Assert.AreEqual(0f, Get(qc, suffix, label), 1e-8);
			// Set to 1, and check
			Set(qc, suffix, label, 1f);
			NUnit.Framework.Assert.AreEqual(1f, Get(qc, suffix, label), 1e-8);
			// Set to 2, and check
			Set(qc, suffix, label, 2f);
			NUnit.Framework.Assert.AreEqual(2f, Get(qc, suffix, label), 1e-8);
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
