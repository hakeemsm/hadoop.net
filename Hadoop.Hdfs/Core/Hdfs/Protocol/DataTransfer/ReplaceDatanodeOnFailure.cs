using System;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>The setting of replace-datanode-on-failure feature.</summary>
	public class ReplaceDatanodeOnFailure
	{
		/// <summary>The replacement policies</summary>
		[System.Serializable]
		public sealed class Policy
		{
			/// <summary>The feature is disabled in the entire site.</summary>
			public static readonly ReplaceDatanodeOnFailure.Policy Disable = new ReplaceDatanodeOnFailure.Policy
				(ReplaceDatanodeOnFailure.Condition.False);

			/// <summary>Never add a new datanode.</summary>
			public static readonly ReplaceDatanodeOnFailure.Policy Never = new ReplaceDatanodeOnFailure.Policy
				(ReplaceDatanodeOnFailure.Condition.False);

			/// <seealso cref="Condition.Default"></seealso>
			public static readonly ReplaceDatanodeOnFailure.Policy Default = new ReplaceDatanodeOnFailure.Policy
				(ReplaceDatanodeOnFailure.Condition.Default);

			/// <summary>Always add a new datanode when an existing datanode is removed.</summary>
			public static readonly ReplaceDatanodeOnFailure.Policy Always = new ReplaceDatanodeOnFailure.Policy
				(ReplaceDatanodeOnFailure.Condition.True);

			private readonly ReplaceDatanodeOnFailure.Condition condition;

			private Policy(ReplaceDatanodeOnFailure.Condition condition)
			{
				this.condition = condition;
			}

			internal ReplaceDatanodeOnFailure.Condition GetCondition()
			{
				return ReplaceDatanodeOnFailure.Policy.condition;
			}
		}

		/// <summary>Datanode replacement condition</summary>
		private abstract class Condition
		{
			private sealed class _Condition_58 : ReplaceDatanodeOnFailure.Condition
			{
				public _Condition_58()
				{
				}

				public override bool Satisfy(short replication, DatanodeInfo[] existings, int nExistings
					, bool isAppend, bool isHflushed)
				{
					return true;
				}
			}

			/// <summary>Return true unconditionally.</summary>
			public const ReplaceDatanodeOnFailure.Condition True = new _Condition_58();

			private sealed class _Condition_67 : ReplaceDatanodeOnFailure.Condition
			{
				public _Condition_67()
				{
				}

				public override bool Satisfy(short replication, DatanodeInfo[] existings, int nExistings
					, bool isAppend, bool isHflushed)
				{
					return false;
				}
			}

			/// <summary>Return false unconditionally.</summary>
			public const ReplaceDatanodeOnFailure.Condition False = new _Condition_67();

			private sealed class _Condition_83 : ReplaceDatanodeOnFailure.Condition
			{
				public _Condition_83()
				{
				}

				public override bool Satisfy(short replication, DatanodeInfo[] existings, int n, 
					bool isAppend, bool isHflushed)
				{
					if (replication < 3)
					{
						return false;
					}
					else
					{
						if (n <= (replication / 2))
						{
							return true;
						}
						else
						{
							return isAppend || isHflushed;
						}
					}
				}
			}

			/// <summary>
			/// DEFAULT condition:
			/// Let r be the replication number.
			/// </summary>
			/// <remarks>
			/// DEFAULT condition:
			/// Let r be the replication number.
			/// Let n be the number of existing datanodes.
			/// Add a new datanode only if r &gt;= 3 and either
			/// (1) floor(r/2) &gt;= n; or
			/// (2) r &gt; n and the block is hflushed/appended.
			/// </remarks>
			public const ReplaceDatanodeOnFailure.Condition Default = new _Condition_83();

			/// <summary>Is the condition satisfied?</summary>
			public abstract bool Satisfy(short replication, DatanodeInfo[] existings, int nExistings
				, bool isAppend, bool isHflushed);
		}

		private static class ConditionConstants
		{
		}

		private readonly ReplaceDatanodeOnFailure.Policy policy;

		private readonly bool bestEffort;

		public ReplaceDatanodeOnFailure(ReplaceDatanodeOnFailure.Policy policy, bool bestEffort
			)
		{
			this.policy = policy;
			this.bestEffort = bestEffort;
		}

		/// <summary>Check if the feature is enabled.</summary>
		public virtual void CheckEnabled()
		{
			if (policy == ReplaceDatanodeOnFailure.Policy.Disable)
			{
				throw new NotSupportedException("This feature is disabled.  Please refer to " + DFSConfigKeys
					.DfsClientWriteReplaceDatanodeOnFailureEnableKey + " configuration property.");
			}
		}

		/// <summary>
		/// Best effort means that the client will try to replace the failed datanode
		/// (provided that the policy is satisfied), however, it will continue the
		/// write operation in case that the datanode replacement also fails.
		/// </summary>
		/// <returns>
		/// Suppose the datanode replacement fails.
		/// false: An exception should be thrown so that the write will fail.
		/// true : The write should be resumed with the remaining datandoes.
		/// </returns>
		public virtual bool IsBestEffort()
		{
			return bestEffort;
		}

		/// <summary>Does it need a replacement according to the policy?</summary>
		public virtual bool Satisfy(short replication, DatanodeInfo[] existings, bool isAppend
			, bool isHflushed)
		{
			int n = existings == null ? 0 : existings.Length;
			if (n == 0 || n >= replication)
			{
				//don't need to add datanode for any policy.
				return false;
			}
			else
			{
				return policy.GetCondition().Satisfy(replication, existings, n, isAppend, isHflushed
					);
			}
		}

		public override string ToString()
		{
			return policy.ToString();
		}

		/// <summary>Get the setting from configuration.</summary>
		public static ReplaceDatanodeOnFailure Get(Configuration conf)
		{
			ReplaceDatanodeOnFailure.Policy policy = GetPolicy(conf);
			bool bestEffort = conf.GetBoolean(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureBestEffortKey
				, DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureBestEffortDefault);
			return new ReplaceDatanodeOnFailure(policy, bestEffort);
		}

		private static ReplaceDatanodeOnFailure.Policy GetPolicy(Configuration conf)
		{
			bool enabled = conf.GetBoolean(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureEnableKey
				, DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureEnableDefault);
			if (!enabled)
			{
				return ReplaceDatanodeOnFailure.Policy.Disable;
			}
			string policy = conf.Get(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailurePolicyKey
				, DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailurePolicyDefault);
			for (int i = 1; i < ReplaceDatanodeOnFailure.Policy.Values().Length; i++)
			{
				ReplaceDatanodeOnFailure.Policy p = ReplaceDatanodeOnFailure.Policy.Values()[i];
				if (Sharpen.Runtime.EqualsIgnoreCase(p.ToString(), policy))
				{
					return p;
				}
			}
			throw new HadoopIllegalArgumentException("Illegal configuration value for " + DFSConfigKeys
				.DfsClientWriteReplaceDatanodeOnFailurePolicyKey + ": " + policy);
		}

		/// <summary>Write the setting to configuration.</summary>
		public static void Write(ReplaceDatanodeOnFailure.Policy policy, bool bestEffort, 
			Configuration conf)
		{
			conf.SetBoolean(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureEnableKey, policy
				 != ReplaceDatanodeOnFailure.Policy.Disable);
			conf.Set(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailurePolicyKey, policy.ToString
				());
			conf.SetBoolean(DFSConfigKeys.DfsClientWriteReplaceDatanodeOnFailureBestEffortKey
				, bestEffort);
		}
	}
}
