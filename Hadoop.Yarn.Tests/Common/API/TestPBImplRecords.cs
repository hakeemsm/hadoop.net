using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestPBImplRecords
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestPBImplRecords));

		private static Dictionary<Type, object> typeValueCache = new Dictionary<Type, object
			>();

		private static Random rand = new Random();

		private static byte[] bytes = new byte[] { (byte)('1'), (byte)('2'), (byte)('3'), 
			(byte)('4') };

		private static object GenTypeValue(Type type)
		{
			object ret = typeValueCache[type];
			if (ret != null)
			{
				return ret;
			}
			// only use positive primitive values
			if (type.Equals(typeof(bool)))
			{
				return rand.NextBoolean();
			}
			else
			{
				if (type.Equals(typeof(byte)))
				{
					return bytes[rand.Next(4)];
				}
				else
				{
					if (type.Equals(typeof(int)))
					{
						return rand.Next(1000000);
					}
					else
					{
						if (type.Equals(typeof(long)))
						{
							return Sharpen.Extensions.ValueOf(rand.Next(1000000));
						}
						else
						{
							if (type.Equals(typeof(float)))
							{
								return rand.NextFloat();
							}
							else
							{
								if (type.Equals(typeof(double)))
								{
									return rand.NextDouble();
								}
								else
								{
									if (type.Equals(typeof(string)))
									{
										return string.Format("%c%c%c", 'a' + rand.Next(26), 'a' + rand.Next(26), 'a' + rand
											.Next(26));
									}
									else
									{
										if (type is Type)
										{
											Type clazz = (Type)type;
											if (clazz.IsArray)
											{
												Type compClass = clazz.GetElementType();
												if (compClass != null)
												{
													ret = System.Array.CreateInstance(compClass, 2);
													Sharpen.Runtime.SetArrayValue(ret, 0, GenTypeValue(compClass));
													Sharpen.Runtime.SetArrayValue(ret, 1, GenTypeValue(compClass));
												}
											}
											else
											{
												if (clazz.IsEnum())
												{
													object[] values = clazz.GetEnumConstants();
													ret = values[rand.Next(values.Length)];
												}
												else
												{
													if (clazz.Equals(typeof(ByteBuffer)))
													{
														// return new ByteBuffer every time
														// to prevent potential side effects
														ByteBuffer buff = ByteBuffer.Allocate(4);
														rand.NextBytes(((byte[])buff.Array()));
														return buff;
													}
												}
											}
										}
										else
										{
											if (type is ParameterizedType)
											{
												ParameterizedType pt = (ParameterizedType)type;
												Type rawType = pt.GetRawType();
												Type[] @params = pt.GetActualTypeArguments();
												// only support EnumSet<T>, List<T>, Set<T>, Map<K,V>
												if (rawType.Equals(typeof(EnumSet)))
												{
													if (@params[0] is Type)
													{
														Type c = (Type)(@params[0]);
														return EnumSet.AllOf(c);
													}
												}
												if (rawType.Equals(typeof(IList)))
												{
													ret = Lists.NewArrayList(GenTypeValue(@params[0]));
												}
												else
												{
													if (rawType.Equals(typeof(Set)))
													{
														ret = Sets.NewHashSet(GenTypeValue(@params[0]));
													}
													else
													{
														if (rawType.Equals(typeof(IDictionary)))
														{
															IDictionary<object, object> map = Maps.NewHashMap();
															map[GenTypeValue(@params[0])] = GenTypeValue(@params[1]);
															ret = map;
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (ret == null)
			{
				throw new ArgumentException("type " + type + " is not supported");
			}
			typeValueCache[type] = ret;
			return ret;
		}

		/// <summary>
		/// this method generate record instance by calling newIntance
		/// using reflection, add register the generated value to typeValueCache
		/// </summary>
		/// <exception cref="System.Exception"/>
		private static object GenerateByNewInstance(Type clazz)
		{
			object ret = typeValueCache[clazz];
			if (ret != null)
			{
				return ret;
			}
			MethodInfo newInstance = null;
			Type[] paramTypes = new Type[0];
			// get newInstance method with most parameters
			foreach (MethodInfo m in clazz.GetMethods())
			{
				int mod = m.GetModifiers();
				if (m.DeclaringType.Equals(clazz) && Modifier.IsPublic(mod) && Modifier.IsStatic(
					mod) && m.Name.Equals("newInstance"))
				{
					Type[] pts = m.GetGenericParameterTypes();
					if (newInstance == null || (pts.Length > paramTypes.Length))
					{
						newInstance = m;
						paramTypes = pts;
					}
				}
			}
			if (newInstance == null)
			{
				throw new ArgumentException("type " + clazz.FullName + " does not have newInstance method"
					);
			}
			object[] args = new object[paramTypes.Length];
			for (int i = 0; i < args.Length; i++)
			{
				args[i] = GenTypeValue(paramTypes[i]);
			}
			ret = newInstance.Invoke(null, args);
			typeValueCache[clazz] = ret;
			return ret;
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			typeValueCache[typeof(LongRange)] = new LongRange(1000, 2000);
			typeValueCache[typeof(URL)] = URL.NewInstance("http", "localhost", 8080, "file0");
			typeValueCache[typeof(SerializedException)] = SerializedException.NewInstance(new 
				IOException("exception for test"));
			GenerateByNewInstance(typeof(LogAggregationContext));
			GenerateByNewInstance(typeof(ApplicationId));
			GenerateByNewInstance(typeof(ApplicationAttemptId));
			GenerateByNewInstance(typeof(ContainerId));
			GenerateByNewInstance(typeof(Resource));
			GenerateByNewInstance(typeof(ResourceBlacklistRequest));
			GenerateByNewInstance(typeof(ResourceOption));
			GenerateByNewInstance(typeof(LocalResource));
			GenerateByNewInstance(typeof(Priority));
			GenerateByNewInstance(typeof(NodeId));
			GenerateByNewInstance(typeof(NodeReport));
			GenerateByNewInstance(typeof(Token));
			GenerateByNewInstance(typeof(NMToken));
			GenerateByNewInstance(typeof(ResourceRequest));
			GenerateByNewInstance(typeof(ApplicationAttemptReport));
			GenerateByNewInstance(typeof(ApplicationResourceUsageReport));
			GenerateByNewInstance(typeof(ApplicationReport));
			GenerateByNewInstance(typeof(Container));
			GenerateByNewInstance(typeof(ContainerLaunchContext));
			GenerateByNewInstance(typeof(ApplicationSubmissionContext));
			GenerateByNewInstance(typeof(ContainerReport));
			GenerateByNewInstance(typeof(ContainerResourceDecrease));
			GenerateByNewInstance(typeof(ContainerResourceIncrease));
			GenerateByNewInstance(typeof(ContainerResourceIncreaseRequest));
			GenerateByNewInstance(typeof(ContainerStatus));
			GenerateByNewInstance(typeof(PreemptionContainer));
			GenerateByNewInstance(typeof(PreemptionResourceRequest));
			GenerateByNewInstance(typeof(PreemptionContainer));
			GenerateByNewInstance(typeof(PreemptionContract));
			GenerateByNewInstance(typeof(StrictPreemptionContract));
			GenerateByNewInstance(typeof(PreemptionMessage));
			GenerateByNewInstance(typeof(StartContainerRequest));
			// genByNewInstance does not apply to QueueInfo, cause
			// it is recursive(has sub queues)
			typeValueCache[typeof(QueueInfo)] = QueueInfo.NewInstance("root", 1.0f, 1.0f, 0.1f
				, null, null, QueueState.Running, ImmutableSet.Of("x", "y"), "x && y");
			GenerateByNewInstance(typeof(QueueUserACLInfo));
			GenerateByNewInstance(typeof(YarnClusterMetrics));
			// for reservation system
			GenerateByNewInstance(typeof(ReservationId));
			GenerateByNewInstance(typeof(ReservationRequest));
			GenerateByNewInstance(typeof(ReservationRequests));
			GenerateByNewInstance(typeof(ReservationDefinition));
		}

		private class GetSetPair
		{
			public string propertyName;

			public MethodInfo getMethod;

			public MethodInfo setMethod;

			public Type type;

			public object testValue;

			public override string ToString()
			{
				return string.Format("{ name=%s, class=%s, value=%s }", this.propertyName, this.type
					, this.testValue);
			}

			internal GetSetPair(TestPBImplRecords _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestPBImplRecords _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private IDictionary<string, TestPBImplRecords.GetSetPair> GetGetSetPairs<R>()
		{
			System.Type recordClass = typeof(R);
			IDictionary<string, TestPBImplRecords.GetSetPair> ret = new Dictionary<string, TestPBImplRecords.GetSetPair
				>();
			MethodInfo[] methods = Sharpen.Runtime.GetDeclaredMethods(recordClass);
			// get all get methods
			for (int i = 0; i < methods.Length; i++)
			{
				MethodInfo m = methods[i];
				int mod = m.GetModifiers();
				if (m.DeclaringType.Equals(recordClass) && Modifier.IsPublic(mod) && (!Modifier.IsStatic
					(mod)))
				{
					string name = m.Name;
					if (name.Equals("getProto"))
					{
						continue;
					}
					if ((name.Length > 3) && name.StartsWith("get") && (Sharpen.Runtime.GetParameterTypes
						(m).Length == 0))
					{
						string propertyName = Sharpen.Runtime.Substring(name, 3);
						Type valueType = m.GetGenericReturnType();
						TestPBImplRecords.GetSetPair p = ret[propertyName];
						if (p == null)
						{
							p = new TestPBImplRecords.GetSetPair(this);
							p.propertyName = propertyName;
							p.type = valueType;
							p.getMethod = m;
							ret[propertyName] = p;
						}
						else
						{
							NUnit.Framework.Assert.Fail("Multiple get method with same name: " + recordClass 
								+ p.propertyName);
						}
					}
				}
			}
			// match get methods with set methods
			for (int i_1 = 0; i_1 < methods.Length; i_1++)
			{
				MethodInfo m = methods[i_1];
				int mod = m.GetModifiers();
				if (m.DeclaringType.Equals(recordClass) && Modifier.IsPublic(mod) && (!Modifier.IsStatic
					(mod)))
				{
					string name = m.Name;
					if (name.StartsWith("set") && (Sharpen.Runtime.GetParameterTypes(m).Length == 1))
					{
						string propertyName = Sharpen.Runtime.Substring(name, 3);
						Type valueType = m.GetGenericParameterTypes()[0];
						TestPBImplRecords.GetSetPair p = ret[propertyName];
						if (p != null && p.type.Equals(valueType))
						{
							p.setMethod = m;
						}
					}
				}
			}
			// exclude incomplete get/set pair, and generate test value
			IEnumerator<KeyValuePair<string, TestPBImplRecords.GetSetPair>> itr = ret.GetEnumerator
				();
			while (itr.HasNext())
			{
				KeyValuePair<string, TestPBImplRecords.GetSetPair> cur = itr.Next();
				TestPBImplRecords.GetSetPair gsp = cur.Value;
				if ((gsp.getMethod == null) || (gsp.setMethod == null))
				{
					Log.Info(string.Format("Exclude protential property: %s\n", gsp.propertyName));
					itr.Remove();
				}
				else
				{
					Log.Info(string.Format("New property: %s type: %s", gsp.ToString(), gsp.type));
					gsp.testValue = GenTypeValue(gsp.type);
					Log.Info(string.Format(" testValue: %s\n", gsp.testValue));
				}
			}
			return ret;
		}

		/// <exception cref="System.Exception"/>
		private void ValidatePBImplRecord<R, P>()
		{
			System.Type recordClass = typeof(R);
			System.Type protoClass = typeof(P);
			Log.Info(string.Format("Validate %s %s\n", recordClass.FullName, protoClass.FullName
				));
			Constructor<R> emptyConstructor = recordClass.GetConstructor();
			Constructor<R> pbConstructor = recordClass.GetConstructor(protoClass);
			MethodInfo getProto = Sharpen.Runtime.GetDeclaredMethod(recordClass, "getProto");
			IDictionary<string, TestPBImplRecords.GetSetPair> getSetPairs = GetGetSetPairs(recordClass
				);
			R origRecord = emptyConstructor.NewInstance();
			foreach (TestPBImplRecords.GetSetPair gsp in getSetPairs.Values)
			{
				gsp.setMethod.Invoke(origRecord, gsp.testValue);
			}
			object ret = getProto.Invoke(origRecord);
			NUnit.Framework.Assert.IsNotNull(recordClass.FullName + "#getProto returns null", 
				ret);
			if (!(protoClass.IsAssignableFrom(ret.GetType())))
			{
				NUnit.Framework.Assert.Fail("Illegal getProto method return type: " + ret.GetType
					());
			}
			R deserRecord = pbConstructor.NewInstance(ret);
			NUnit.Framework.Assert.AreEqual("whole " + recordClass + " records should be equal"
				, origRecord, deserRecord);
			foreach (TestPBImplRecords.GetSetPair gsp_1 in getSetPairs.Values)
			{
				object origValue = gsp_1.getMethod.Invoke(origRecord);
				object deserValue = gsp_1.getMethod.Invoke(deserRecord);
				NUnit.Framework.Assert.AreEqual("property " + recordClass.FullName + "#" + gsp_1.
					propertyName + " should be equal", origValue, deserValue);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocateRequestPBImpl()
		{
			ValidatePBImplRecord<AllocateRequestPBImpl, YarnServiceProtos.AllocateRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocateResponsePBImpl()
		{
			ValidatePBImplRecord<AllocateResponsePBImpl, YarnServiceProtos.AllocateResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelDelegationTokenRequestPBImpl()
		{
			ValidatePBImplRecord<CancelDelegationTokenRequestPBImpl, SecurityProtos.CancelDelegationTokenRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelDelegationTokenResponsePBImpl()
		{
			ValidatePBImplRecord<CancelDelegationTokenResponsePBImpl, SecurityProtos.CancelDelegationTokenResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFinishApplicationMasterRequestPBImpl()
		{
			ValidatePBImplRecord<FinishApplicationMasterRequestPBImpl, YarnServiceProtos.FinishApplicationMasterRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFinishApplicationMasterResponsePBImpl()
		{
			ValidatePBImplRecord<FinishApplicationMasterResponsePBImpl, YarnServiceProtos.FinishApplicationMasterResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptReportRequestPBImpl()
		{
			ValidatePBImplRecord<GetApplicationAttemptReportRequestPBImpl, YarnServiceProtos.GetApplicationAttemptReportRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptReportResponsePBImpl()
		{
			ValidatePBImplRecord<GetApplicationAttemptReportResponsePBImpl, YarnServiceProtos.GetApplicationAttemptReportResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptsRequestPBImpl()
		{
			ValidatePBImplRecord<GetApplicationAttemptsRequestPBImpl, YarnServiceProtos.GetApplicationAttemptsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptsResponsePBImpl()
		{
			ValidatePBImplRecord<GetApplicationAttemptsResponsePBImpl, YarnServiceProtos.GetApplicationAttemptsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReportRequestPBImpl()
		{
			ValidatePBImplRecord<GetApplicationReportRequestPBImpl, YarnServiceProtos.GetApplicationReportRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReportResponsePBImpl()
		{
			ValidatePBImplRecord<GetApplicationReportResponsePBImpl, YarnServiceProtos.GetApplicationReportResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationsRequestPBImpl()
		{
			ValidatePBImplRecord<GetApplicationsRequestPBImpl, YarnServiceProtos.GetApplicationsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationsResponsePBImpl()
		{
			ValidatePBImplRecord<GetApplicationsResponsePBImpl, YarnServiceProtos.GetApplicationsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterMetricsRequestPBImpl()
		{
			ValidatePBImplRecord<GetClusterMetricsRequestPBImpl, YarnServiceProtos.GetClusterMetricsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterMetricsResponsePBImpl()
		{
			ValidatePBImplRecord<GetClusterMetricsResponsePBImpl, YarnServiceProtos.GetClusterMetricsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodesRequestPBImpl()
		{
			ValidatePBImplRecord<GetClusterNodesRequestPBImpl, YarnServiceProtos.GetClusterNodesRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodesResponsePBImpl()
		{
			ValidatePBImplRecord<GetClusterNodesResponsePBImpl, YarnServiceProtos.GetClusterNodesResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerReportRequestPBImpl()
		{
			ValidatePBImplRecord<GetContainerReportRequestPBImpl, YarnServiceProtos.GetContainerReportRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerReportResponsePBImpl()
		{
			ValidatePBImplRecord<GetContainerReportResponsePBImpl, YarnServiceProtos.GetContainerReportResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainersRequestPBImpl()
		{
			ValidatePBImplRecord<GetContainersRequestPBImpl, YarnServiceProtos.GetContainersRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainersResponsePBImpl()
		{
			ValidatePBImplRecord<GetContainersResponsePBImpl, YarnServiceProtos.GetContainersResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerStatusesRequestPBImpl()
		{
			ValidatePBImplRecord<GetContainerStatusesRequestPBImpl, YarnServiceProtos.GetContainerStatusesRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerStatusesResponsePBImpl()
		{
			ValidatePBImplRecord<GetContainerStatusesResponsePBImpl, YarnServiceProtos.GetContainerStatusesResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDelegationTokenRequestPBImpl()
		{
			ValidatePBImplRecord<GetDelegationTokenRequestPBImpl, SecurityProtos.GetDelegationTokenRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDelegationTokenResponsePBImpl()
		{
			ValidatePBImplRecord<GetDelegationTokenResponsePBImpl, SecurityProtos.GetDelegationTokenResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNewApplicationRequestPBImpl()
		{
			ValidatePBImplRecord<GetNewApplicationRequestPBImpl, YarnServiceProtos.GetNewApplicationRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNewApplicationResponsePBImpl()
		{
			ValidatePBImplRecord<GetNewApplicationResponsePBImpl, YarnServiceProtos.GetNewApplicationResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueInfoRequestPBImpl()
		{
			ValidatePBImplRecord<GetQueueInfoRequestPBImpl, YarnServiceProtos.GetQueueInfoRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueInfoResponsePBImpl()
		{
			ValidatePBImplRecord<GetQueueInfoResponsePBImpl, YarnServiceProtos.GetQueueInfoResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueUserAclsInfoRequestPBImpl()
		{
			ValidatePBImplRecord<GetQueueUserAclsInfoRequestPBImpl, YarnServiceProtos.GetQueueUserAclsInfoRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueUserAclsInfoResponsePBImpl()
		{
			ValidatePBImplRecord<GetQueueUserAclsInfoResponsePBImpl, YarnServiceProtos.GetQueueUserAclsInfoResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillApplicationRequestPBImpl()
		{
			ValidatePBImplRecord<KillApplicationRequestPBImpl, YarnServiceProtos.KillApplicationRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillApplicationResponsePBImpl()
		{
			ValidatePBImplRecord<KillApplicationResponsePBImpl, YarnServiceProtos.KillApplicationResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveApplicationAcrossQueuesRequestPBImpl()
		{
			ValidatePBImplRecord<MoveApplicationAcrossQueuesRequestPBImpl, YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveApplicationAcrossQueuesResponsePBImpl()
		{
			ValidatePBImplRecord<MoveApplicationAcrossQueuesResponsePBImpl, YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRegisterApplicationMasterRequestPBImpl()
		{
			ValidatePBImplRecord<RegisterApplicationMasterRequestPBImpl, YarnServiceProtos.RegisterApplicationMasterRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRegisterApplicationMasterResponsePBImpl()
		{
			ValidatePBImplRecord<RegisterApplicationMasterResponsePBImpl, YarnServiceProtos.RegisterApplicationMasterResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenewDelegationTokenRequestPBImpl()
		{
			ValidatePBImplRecord<RenewDelegationTokenRequestPBImpl, SecurityProtos.RenewDelegationTokenRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenewDelegationTokenResponsePBImpl()
		{
			ValidatePBImplRecord<RenewDelegationTokenResponsePBImpl, SecurityProtos.RenewDelegationTokenResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartContainerRequestPBImpl()
		{
			ValidatePBImplRecord<StartContainerRequestPBImpl, YarnServiceProtos.StartContainerRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartContainersRequestPBImpl()
		{
			ValidatePBImplRecord<StartContainersRequestPBImpl, YarnServiceProtos.StartContainersRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartContainersResponsePBImpl()
		{
			ValidatePBImplRecord<StartContainersResponsePBImpl, YarnServiceProtos.StartContainersResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopContainersRequestPBImpl()
		{
			ValidatePBImplRecord<StopContainersRequestPBImpl, YarnServiceProtos.StopContainersRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopContainersResponsePBImpl()
		{
			ValidatePBImplRecord<StopContainersResponsePBImpl, YarnServiceProtos.StopContainersResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSubmitApplicationRequestPBImpl()
		{
			ValidatePBImplRecord<SubmitApplicationRequestPBImpl, YarnServiceProtos.SubmitApplicationRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSubmitApplicationResponsePBImpl()
		{
			ValidatePBImplRecord<SubmitApplicationResponsePBImpl, YarnServiceProtos.SubmitApplicationResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestApplicationAttemptIdPBImpl()
		{
			// ignore cause ApplicationIdPBImpl is immutable
			ValidatePBImplRecord<ApplicationAttemptIdPBImpl, YarnProtos.ApplicationAttemptIdProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationAttemptReportPBImpl()
		{
			ValidatePBImplRecord<ApplicationAttemptReportPBImpl, YarnProtos.ApplicationAttemptReportProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestApplicationIdPBImpl()
		{
			// ignore cause ApplicationIdPBImpl is immutable
			ValidatePBImplRecord<ApplicationIdPBImpl, YarnProtos.ApplicationIdProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationReportPBImpl()
		{
			ValidatePBImplRecord<ApplicationReportPBImpl, YarnProtos.ApplicationReportProto>(
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationResourceUsageReportPBImpl()
		{
			ValidatePBImplRecord<ApplicationResourceUsageReportPBImpl, YarnProtos.ApplicationResourceUsageReportProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationSubmissionContextPBImpl()
		{
			ValidatePBImplRecord<ApplicationSubmissionContextPBImpl, YarnProtos.ApplicationSubmissionContextProto
				>();
			ApplicationSubmissionContext ctx = ApplicationSubmissionContext.NewInstance(null, 
				null, null, null, null, false, false, 0, Resources.None(), null, false, null, null
				);
			NUnit.Framework.Assert.IsNotNull(ctx.GetResource());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestContainerIdPBImpl()
		{
			// ignore cause ApplicationIdPBImpl is immutable
			ValidatePBImplRecord<ContainerIdPBImpl, YarnProtos.ContainerIdProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchContextPBImpl()
		{
			ValidatePBImplRecord<ContainerLaunchContextPBImpl, YarnProtos.ContainerLaunchContextProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerPBImpl()
		{
			ValidatePBImplRecord<ContainerPBImpl, YarnProtos.ContainerProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerReportPBImpl()
		{
			ValidatePBImplRecord<ContainerReportPBImpl, YarnProtos.ContainerReportProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerResourceDecreasePBImpl()
		{
			ValidatePBImplRecord<ContainerResourceDecreasePBImpl, YarnProtos.ContainerResourceDecreaseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerResourceIncreasePBImpl()
		{
			ValidatePBImplRecord<ContainerResourceIncreasePBImpl, YarnProtos.ContainerResourceIncreaseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerResourceIncreaseRequestPBImpl()
		{
			ValidatePBImplRecord<ContainerResourceIncreaseRequestPBImpl, YarnProtos.ContainerResourceIncreaseRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerStatusPBImpl()
		{
			ValidatePBImplRecord<ContainerStatusPBImpl, YarnProtos.ContainerStatusProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalResourcePBImpl()
		{
			ValidatePBImplRecord<LocalResourcePBImpl, YarnProtos.LocalResourceProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNMTokenPBImpl()
		{
			ValidatePBImplRecord<NMTokenPBImpl, YarnServiceProtos.NMTokenProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestNodeIdPBImpl()
		{
			// ignore cause ApplicationIdPBImpl is immutable
			ValidatePBImplRecord<NodeIdPBImpl, YarnProtos.NodeIdProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeReportPBImpl()
		{
			ValidatePBImplRecord<NodeReportPBImpl, YarnProtos.NodeReportProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionContainerPBImpl()
		{
			ValidatePBImplRecord<PreemptionContainerPBImpl, YarnProtos.PreemptionContainerProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionContractPBImpl()
		{
			ValidatePBImplRecord<PreemptionContractPBImpl, YarnProtos.PreemptionContractProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionMessagePBImpl()
		{
			ValidatePBImplRecord<PreemptionMessagePBImpl, YarnProtos.PreemptionMessageProto>(
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionResourceRequestPBImpl()
		{
			ValidatePBImplRecord<PreemptionResourceRequestPBImpl, YarnProtos.PreemptionResourceRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPriorityPBImpl()
		{
			ValidatePBImplRecord<PriorityPBImpl, YarnProtos.PriorityProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueInfoPBImpl()
		{
			ValidatePBImplRecord<QueueInfoPBImpl, YarnProtos.QueueInfoProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueUserACLInfoPBImpl()
		{
			ValidatePBImplRecord<QueueUserACLInfoPBImpl, YarnProtos.QueueUserACLInfoProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceBlacklistRequestPBImpl()
		{
			ValidatePBImplRecord<ResourceBlacklistRequestPBImpl, YarnProtos.ResourceBlacklistRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestResourceOptionPBImpl()
		{
			// ignore as ResourceOptionPBImpl is immutable
			ValidatePBImplRecord<ResourceOptionPBImpl, YarnProtos.ResourceOptionProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourcePBImpl()
		{
			ValidatePBImplRecord<ResourcePBImpl, YarnProtos.ResourceProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceRequestPBImpl()
		{
			ValidatePBImplRecord<ResourceRequestPBImpl, YarnProtos.ResourceRequestProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSerializedExceptionPBImpl()
		{
			ValidatePBImplRecord<SerializedExceptionPBImpl, YarnProtos.SerializedExceptionProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStrictPreemptionContractPBImpl()
		{
			ValidatePBImplRecord<StrictPreemptionContractPBImpl, YarnProtos.StrictPreemptionContractProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenPBImpl()
		{
			ValidatePBImplRecord<TokenPBImpl, SecurityProtos.TokenProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestURLPBImpl()
		{
			ValidatePBImplRecord<URLPBImpl, YarnProtos.URLProto>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestYarnClusterMetricsPBImpl()
		{
			ValidatePBImplRecord<YarnClusterMetricsPBImpl, YarnProtos.YarnClusterMetricsProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshAdminAclsRequestPBImpl()
		{
			ValidatePBImplRecord<RefreshAdminAclsRequestPBImpl, YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshAdminAclsResponsePBImpl()
		{
			ValidatePBImplRecord<RefreshAdminAclsResponsePBImpl, YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshNodesRequestPBImpl()
		{
			ValidatePBImplRecord<RefreshNodesRequestPBImpl, YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshNodesResponsePBImpl()
		{
			ValidatePBImplRecord<RefreshNodesResponsePBImpl, YarnServerResourceManagerServiceProtos.RefreshNodesResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesRequestPBImpl()
		{
			ValidatePBImplRecord<RefreshQueuesRequestPBImpl, YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesResponsePBImpl()
		{
			ValidatePBImplRecord<RefreshQueuesResponsePBImpl, YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshServiceAclsRequestPBImpl()
		{
			ValidatePBImplRecord<RefreshServiceAclsRequestPBImpl, YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshServiceAclsResponsePBImpl()
		{
			ValidatePBImplRecord<RefreshServiceAclsResponsePBImpl, YarnServerResourceManagerServiceProtos.RefreshServiceAclsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroupsConfigurationRequestPBImpl()
		{
			ValidatePBImplRecord<RefreshSuperUserGroupsConfigurationRequestPBImpl, YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroupsConfigurationResponsePBImpl()
		{
			ValidatePBImplRecord<RefreshSuperUserGroupsConfigurationResponsePBImpl, YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshUserToGroupsMappingsRequestPBImpl()
		{
			ValidatePBImplRecord<RefreshUserToGroupsMappingsRequestPBImpl, YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshUserToGroupsMappingsResponsePBImpl()
		{
			ValidatePBImplRecord<RefreshUserToGroupsMappingsResponsePBImpl, YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateNodeResourceRequestPBImpl()
		{
			ValidatePBImplRecord<UpdateNodeResourceRequestPBImpl, YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateNodeResourceResponsePBImpl()
		{
			ValidatePBImplRecord<UpdateNodeResourceResponsePBImpl, YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationSubmissionRequestPBImpl()
		{
			ValidatePBImplRecord<ReservationSubmissionRequestPBImpl, YarnServiceProtos.ReservationSubmissionRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationSubmissionResponsePBImpl()
		{
			ValidatePBImplRecord<ReservationSubmissionResponsePBImpl, YarnServiceProtos.ReservationSubmissionResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationUpdateRequestPBImpl()
		{
			ValidatePBImplRecord<ReservationUpdateRequestPBImpl, YarnServiceProtos.ReservationUpdateRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationUpdateResponsePBImpl()
		{
			ValidatePBImplRecord<ReservationUpdateResponsePBImpl, YarnServiceProtos.ReservationUpdateResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationDeleteRequestPBImpl()
		{
			ValidatePBImplRecord<ReservationDeleteRequestPBImpl, YarnServiceProtos.ReservationDeleteRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationDeleteResponsePBImpl()
		{
			ValidatePBImplRecord<ReservationDeleteResponsePBImpl, YarnServiceProtos.ReservationDeleteResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddToClusterNodeLabelsRequestPBImpl()
		{
			ValidatePBImplRecord<AddToClusterNodeLabelsRequestPBImpl, YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddToClusterNodeLabelsResponsePBImpl()
		{
			ValidatePBImplRecord<AddToClusterNodeLabelsResponsePBImpl, YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveFromClusterNodeLabelsRequestPBImpl()
		{
			ValidatePBImplRecord<RemoveFromClusterNodeLabelsRequestPBImpl, YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveFromClusterNodeLabelsResponsePBImpl()
		{
			ValidatePBImplRecord<RemoveFromClusterNodeLabelsResponsePBImpl, YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodeLabelsRequestPBImpl()
		{
			ValidatePBImplRecord<GetClusterNodeLabelsRequestPBImpl, YarnServiceProtos.GetClusterNodeLabelsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodeLabelsResponsePBImpl()
		{
			ValidatePBImplRecord<GetClusterNodeLabelsResponsePBImpl, YarnServiceProtos.GetClusterNodeLabelsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceLabelsOnNodeRequestPBImpl()
		{
			ValidatePBImplRecord<ReplaceLabelsOnNodeRequestPBImpl, YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceLabelsOnNodeResponsePBImpl()
		{
			ValidatePBImplRecord<ReplaceLabelsOnNodeResponsePBImpl, YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNodeToLabelsRequestPBImpl()
		{
			ValidatePBImplRecord<GetNodesToLabelsRequestPBImpl, YarnServiceProtos.GetNodesToLabelsRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNodeToLabelsResponsePBImpl()
		{
			ValidatePBImplRecord<GetNodesToLabelsResponsePBImpl, YarnServiceProtos.GetNodesToLabelsResponseProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetLabelsToNodesRequestPBImpl()
		{
			ValidatePBImplRecord<GetLabelsToNodesRequestPBImpl, YarnServiceProtos.GetLabelsToNodesRequestProto
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetLabelsToNodesResponsePBImpl()
		{
			ValidatePBImplRecord<GetLabelsToNodesResponsePBImpl, YarnServiceProtos.GetLabelsToNodesResponseProto
				>();
		}
	}
}
