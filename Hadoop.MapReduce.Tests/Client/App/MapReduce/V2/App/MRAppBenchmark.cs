using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class MRAppBenchmark
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <summary>Runs memory and time benchmark with Mock MRApp.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void Run(MRApp app)
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Warn);
			long startTime = Runtime.CurrentTimeMillis();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			while (!job.GetReport().GetJobState().Equals(JobState.Succeeded))
			{
				PrintStat(job, startTime);
				Sharpen.Thread.Sleep(2000);
			}
			PrintStat(job, startTime);
		}

		/// <exception cref="System.Exception"/>
		private void PrintStat(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, long startTime
			)
		{
			long currentTime = Runtime.CurrentTimeMillis();
			Runtime.GetRuntime().Gc();
			long mem = Runtime.GetRuntime().TotalMemory() - Runtime.GetRuntime().FreeMemory();
			System.Console.Out.WriteLine("JobState:" + job.GetState() + " CompletedMaps:" + job
				.GetCompletedMaps() + " CompletedReduces:" + job.GetCompletedReduces() + " Memory(total-free)(KB):"
				 + mem / 1024 + " ElapsedTime(ms):" + (currentTime - startTime));
		}

		internal class ThrottledMRApp : MRApp
		{
			internal int maxConcurrentRunningTasks;

			internal volatile int concurrentRunningTasks;

			internal ThrottledMRApp(int maps, int reduces, int maxConcurrentRunningTasks)
				: base(maps, reduces, true, "ThrottledMRApp", true)
			{
				//Throttles the maximum number of concurrent running tasks.
				//This affects the memory requirement since 
				//org.apache.hadoop.mapred.MapTask/ReduceTask is loaded in memory for all
				//running task and discarded once the task is launched.
				this.maxConcurrentRunningTasks = maxConcurrentRunningTasks;
			}

			protected internal override void AttemptLaunched(TaskAttemptId attemptID)
			{
				base.AttemptLaunched(attemptID);
				//the task is launched and sends done immediately
				concurrentRunningTasks--;
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				return new MRAppBenchmark.ThrottledMRApp.ThrottledContainerAllocator(this);
			}

			internal class ThrottledContainerAllocator : AbstractService, ContainerAllocator
			{
				private int containerCount;

				private Sharpen.Thread thread;

				private BlockingQueue<ContainerAllocatorEvent> eventQueue = new LinkedBlockingQueue
					<ContainerAllocatorEvent>();

				public ThrottledContainerAllocator(ThrottledMRApp _enclosing)
					: base("ThrottledContainerAllocator")
				{
					this._enclosing = _enclosing;
				}

				public virtual void Handle(ContainerAllocatorEvent @event)
				{
					try
					{
						this.eventQueue.Put(@event);
					}
					catch (Exception e)
					{
						throw new YarnRuntimeException(e);
					}
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStart()
				{
					this.thread = new Sharpen.Thread(new _Runnable_134(this));
					//System.out.println("Allocating " + containerCount);
					this.thread.Start();
					base.ServiceStart();
				}

				private sealed class _Runnable_134 : Runnable
				{
					public _Runnable_134(ThrottledContainerAllocator _enclosing)
					{
						this._enclosing = _enclosing;
					}

					public void Run()
					{
						ContainerAllocatorEvent @event = null;
						while (!Sharpen.Thread.CurrentThread().IsInterrupted())
						{
							try
							{
								if (this._enclosing._enclosing.concurrentRunningTasks < this._enclosing._enclosing
									.maxConcurrentRunningTasks)
								{
									@event = this._enclosing.eventQueue.Take();
									ContainerId cId = ContainerId.NewContainerId(this._enclosing._enclosing.GetContext
										().GetApplicationAttemptId(), this._enclosing.containerCount++);
									Container container = MRAppBenchmark.recordFactory.NewRecordInstance<Container>();
									container.SetId(cId);
									NodeId nodeId = NodeId.NewInstance("dummy", 1234);
									container.SetNodeId(nodeId);
									container.SetContainerToken(null);
									container.SetNodeHttpAddress("localhost:8042");
									this._enclosing._enclosing.GetContext().GetEventHandler().Handle(new TaskAttemptContainerAssignedEvent
										(@event.GetAttemptID(), container, null));
									this._enclosing._enclosing.concurrentRunningTasks++;
								}
								else
								{
									Sharpen.Thread.Sleep(1000);
								}
							}
							catch (Exception)
							{
								System.Console.Out.WriteLine("Returning, interrupted");
								return;
							}
						}
					}

					private readonly ThrottledContainerAllocator _enclosing;
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStop()
				{
					if (this.thread != null)
					{
						this.thread.Interrupt();
					}
					base.ServiceStop();
				}

				private readonly ThrottledMRApp _enclosing;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Benchmark1()
		{
			int maps = 100;
			// Adjust for benchmarking. Start with thousands.
			int reduces = 0;
			System.Console.Out.WriteLine("Running benchmark with maps:" + maps + " reduces:" 
				+ reduces);
			Run(new _MRApp_190(this, maps, reduces, true, this.GetType().FullName, true));
		}

		private sealed class _MRApp_190 : MRApp
		{
			public _MRApp_190(MRAppBenchmark _enclosing, int baseArg1, int baseArg2, bool baseArg3
				, string baseArg4, bool baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
				this._enclosing = _enclosing;
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				return new _RMContainerAllocator_195(this, clientService, context);
			}

			private sealed class _RMContainerAllocator_195 : RMContainerAllocator
			{
				public _RMContainerAllocator_195(_MRApp_190 _enclosing, ClientService baseArg1, AppContext
					 baseArg2)
					: base(baseArg1, baseArg2)
				{
					this._enclosing = _enclosing;
				}

				protected internal override ApplicationMasterProtocol CreateSchedulerProxy()
				{
					return new _ApplicationMasterProtocol_198(this);
				}

				private sealed class _ApplicationMasterProtocol_198 : ApplicationMasterProtocol
				{
					public _ApplicationMasterProtocol_198(_RMContainerAllocator_195 _enclosing)
					{
						this._enclosing = _enclosing;
					}

					/// <exception cref="System.IO.IOException"/>
					public RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
						 request)
					{
						RegisterApplicationMasterResponse response = Org.Apache.Hadoop.Yarn.Util.Records.
							NewRecord<RegisterApplicationMasterResponse>();
						response.SetMaximumResourceCapability(Resource.NewInstance(10240, 1));
						return response;
					}

					/// <exception cref="System.IO.IOException"/>
					public FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
						 request)
					{
						FinishApplicationMasterResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
							<FinishApplicationMasterResponse>();
						return response;
					}

					/// <exception cref="System.IO.IOException"/>
					public AllocateResponse Allocate(AllocateRequest request)
					{
						AllocateResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateResponse
							>();
						IList<ResourceRequest> askList = request.GetAskList();
						IList<Container> containers = new AList<Container>();
						foreach (ResourceRequest req in askList)
						{
							if (!ResourceRequest.IsAnyLocation(req.GetResourceName()))
							{
								continue;
							}
							int numContainers = req.GetNumContainers();
							for (int i = 0; i < numContainers; i++)
							{
								ContainerId containerId = ContainerId.NewContainerId(this._enclosing.GetContext()
									.GetApplicationAttemptId(), request.GetResponseId() + i);
								containers.AddItem(Container.NewInstance(containerId, NodeId.NewInstance("host" +
									 containerId.GetContainerId(), 2345), "host" + containerId.GetContainerId() + ":5678"
									, req.GetCapability(), req.GetPriority(), null));
							}
						}
						response.SetAllocatedContainers(containers);
						response.SetResponseId(request.GetResponseId() + 1);
						response.SetNumClusterNodes(350);
						return response;
					}

					private readonly _RMContainerAllocator_195 _enclosing;
				}

				private readonly _MRApp_190 _enclosing;
			}

			private readonly MRAppBenchmark _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Benchmark2()
		{
			int maps = 100;
			// Adjust for benchmarking, start with a couple of thousands
			int reduces = 50;
			int maxConcurrentRunningTasks = 500;
			System.Console.Out.WriteLine("Running benchmark with throttled running tasks with "
				 + "maxConcurrentRunningTasks:" + maxConcurrentRunningTasks + " maps:" + maps + 
				" reduces:" + reduces);
			Run(new MRAppBenchmark.ThrottledMRApp(maps, reduces, maxConcurrentRunningTasks));
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			MRAppBenchmark benchmark = new MRAppBenchmark();
			benchmark.Benchmark1();
			benchmark.Benchmark2();
		}
	}
}
