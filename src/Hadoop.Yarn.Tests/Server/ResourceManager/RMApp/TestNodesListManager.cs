using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class TestNodesListManager
	{
		internal AList<ApplicationId> applist = new AList<ApplicationId>();

		// To hold list of application for which event was received
		/// <exception cref="System.Exception"/>
		public virtual void TestNodeUsableEvent()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			Dispatcher dispatcher = GetDispatcher();
			YarnConfiguration conf = new YarnConfiguration();
			MockRM rm = new _MockRM_62(dispatcher, conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("h1:1234", 28000);
			NodesListManager nodesListManager = rm.GetNodesListManager();
			Resource clusterResource = Resource.NewInstance(28000, 8);
			RMNode rmnode = MockNodes.NewNodeInfo(1, clusterResource);
			// Create killing APP
			RMApp killrmApp = rm.SubmitApp(200);
			rm.KillApp(killrmApp.GetApplicationId());
			rm.WaitForState(killrmApp.GetApplicationId(), RMAppState.Killed);
			// Create finish APP
			RMApp finshrmApp = rm.SubmitApp(2000);
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = finshrmApp.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			nm1.NodeHeartbeat(attempt.GetAppAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			// Create submitted App
			RMApp subrmApp = rm.SubmitApp(200);
			// Fire Event for NODE_USABLE
			nodesListManager.Handle(new NodesListManagerEvent(NodesListManagerEventType.NodeUsable
				, rmnode));
			if (applist.Count > 0)
			{
				NUnit.Framework.Assert.IsTrue("Event based on running app expected " + subrmApp.GetApplicationId
					(), applist.Contains(subrmApp.GetApplicationId()));
				NUnit.Framework.Assert.IsFalse("Event based on finish app not expected " + finshrmApp
					.GetApplicationId(), applist.Contains(finshrmApp.GetApplicationId()));
				NUnit.Framework.Assert.IsFalse("Event based on killed app not expected " + killrmApp
					.GetApplicationId(), applist.Contains(killrmApp.GetApplicationId()));
			}
			else
			{
				NUnit.Framework.Assert.Fail("Events received should have beeen more than 1");
			}
			applist.Clear();
			// Fire Event for NODE_UNUSABLE
			nodesListManager.Handle(new NodesListManagerEvent(NodesListManagerEventType.NodeUnusable
				, rmnode));
			if (applist.Count > 0)
			{
				NUnit.Framework.Assert.IsTrue("Event based on running app expected " + subrmApp.GetApplicationId
					(), applist.Contains(subrmApp.GetApplicationId()));
				NUnit.Framework.Assert.IsFalse("Event based on finish app not expected " + finshrmApp
					.GetApplicationId(), applist.Contains(finshrmApp.GetApplicationId()));
				NUnit.Framework.Assert.IsFalse("Event based on killed app not expected " + killrmApp
					.GetApplicationId(), applist.Contains(killrmApp.GetApplicationId()));
			}
			else
			{
				NUnit.Framework.Assert.Fail("Events received should have beeen more than 1");
			}
		}

		private sealed class _MockRM_62 : MockRM
		{
			public _MockRM_62(Dispatcher dispatcher, Configuration baseArg1)
				: base(baseArg1)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly Dispatcher dispatcher;
		}

		/*
		* Create dispatcher object
		*/
		private Dispatcher GetDispatcher()
		{
			Dispatcher dispatcher = new _AsyncDispatcher_137();
			return dispatcher;
		}

		private sealed class _AsyncDispatcher_137 : AsyncDispatcher
		{
			public _AsyncDispatcher_137()
			{
			}

			public override EventHandler GetEventHandler()
			{
				EventHandler handler = Org.Mockito.Mockito.Spy(base.GetEventHandler());
				Org.Mockito.Mockito.DoNothing().When(handler).Handle(Matchers.ArgThat(new _T1409426889
					(this)));
				return handler;
			}

			internal class _T1409426889 : ArgumentMatcher<AbstractEvent>
			{
				public override bool Matches(object argument)
				{
					if (argument is RMAppNodeUpdateEvent)
					{
						ApplicationId appid = ((RMAppNodeUpdateEvent)argument).GetApplicationId();
						this._enclosing._enclosing.applist.AddItem(appid);
					}
					return false;
				}

				internal _T1409426889(_AsyncDispatcher_137 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly _AsyncDispatcher_137 _enclosing;
			}
		}
	}
}
