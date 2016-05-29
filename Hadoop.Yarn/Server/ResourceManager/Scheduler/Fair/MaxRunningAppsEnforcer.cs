using System;
using System.Collections;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// Handles tracking and enforcement for user and queue maxRunningApps
	/// constraints
	/// </summary>
	public class MaxRunningAppsEnforcer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FairScheduler));

		private readonly FairScheduler scheduler;

		private readonly IDictionary<string, int> usersNumRunnableApps;

		[VisibleForTesting]
		internal readonly ListMultimap<string, FSAppAttempt> usersNonRunnableApps;

		public MaxRunningAppsEnforcer(FairScheduler scheduler)
		{
			// Tracks the number of running applications by user.
			this.scheduler = scheduler;
			this.usersNumRunnableApps = new Dictionary<string, int>();
			this.usersNonRunnableApps = ArrayListMultimap.Create();
		}

		/// <summary>
		/// Checks whether making the application runnable would exceed any
		/// maxRunningApps limits.
		/// </summary>
		public virtual bool CanAppBeRunnable(FSQueue queue, string user)
		{
			AllocationConfiguration allocConf = scheduler.GetAllocationConfiguration();
			int userNumRunnable = usersNumRunnableApps[user];
			if (userNumRunnable == null)
			{
				userNumRunnable = 0;
			}
			if (userNumRunnable >= allocConf.GetUserMaxApps(user))
			{
				return false;
			}
			// Check queue and all parent queues
			while (queue != null)
			{
				int queueMaxApps = allocConf.GetQueueMaxApps(queue.GetName());
				if (queue.GetNumRunnableApps() >= queueMaxApps)
				{
					return false;
				}
				queue = queue.GetParent();
			}
			return true;
		}

		/// <summary>
		/// Tracks the given new runnable app for purposes of maintaining max running
		/// app limits.
		/// </summary>
		public virtual void TrackRunnableApp(FSAppAttempt app)
		{
			string user = app.GetUser();
			FSLeafQueue queue = ((FSLeafQueue)app.GetQueue());
			// Increment running counts for all parent queues
			FSParentQueue parent = queue.GetParent();
			while (parent != null)
			{
				parent.IncrementRunnableApps();
				parent = parent.GetParent();
			}
			int userNumRunnable = usersNumRunnableApps[user];
			usersNumRunnableApps[user] = (userNumRunnable == null ? 0 : userNumRunnable) + 1;
		}

		/// <summary>
		/// Tracks the given new non runnable app so that it can be made runnable when
		/// it would not violate max running app limits.
		/// </summary>
		public virtual void TrackNonRunnableApp(FSAppAttempt app)
		{
			string user = app.GetUser();
			usersNonRunnableApps.Put(user, app);
		}

		/// <summary>
		/// This is called after reloading the allocation configuration when the
		/// scheduler is reinitilized
		/// Checks to see whether any non-runnable applications become runnable
		/// now that the max running apps of given queue has been changed
		/// Runs in O(n) where n is the number of apps that are non-runnable and in
		/// the queues that went from having no slack to having slack.
		/// </summary>
		public virtual void UpdateRunnabilityOnReload()
		{
			FSParentQueue rootQueue = scheduler.GetQueueManager().GetRootQueue();
			IList<IList<FSAppAttempt>> appsNowMaybeRunnable = new AList<IList<FSAppAttempt>>(
				);
			GatherPossiblyRunnableAppLists(rootQueue, appsNowMaybeRunnable);
			UpdateAppsRunnability(appsNowMaybeRunnable, int.MaxValue);
		}

		/// <summary>
		/// Checks to see whether any other applications runnable now that the given
		/// application has been removed from the given queue.
		/// </summary>
		/// <remarks>
		/// Checks to see whether any other applications runnable now that the given
		/// application has been removed from the given queue.  And makes them so.
		/// Runs in O(n log(n)) where n is the number of queues that are under the
		/// highest queue that went from having no slack to having slack.
		/// </remarks>
		public virtual void UpdateRunnabilityOnAppRemoval(FSAppAttempt app, FSLeafQueue queue
			)
		{
			AllocationConfiguration allocConf = scheduler.GetAllocationConfiguration();
			// childqueueX might have no pending apps itself, but if a queue higher up
			// in the hierarchy parentqueueY has a maxRunningApps set, an app completion
			// in childqueueX could allow an app in some other distant child of
			// parentqueueY to become runnable.
			// An app removal will only possibly allow another app to become runnable if
			// the queue was already at its max before the removal.
			// Thus we find the ancestor queue highest in the tree for which the app
			// that was at its maxRunningApps before the removal.
			FSQueue highestQueueWithAppsNowRunnable = (queue.GetNumRunnableApps() == allocConf
				.GetQueueMaxApps(queue.GetName()) - 1) ? queue : null;
			FSParentQueue parent = queue.GetParent();
			while (parent != null)
			{
				if (parent.GetNumRunnableApps() == allocConf.GetQueueMaxApps(parent.GetName()) - 
					1)
				{
					highestQueueWithAppsNowRunnable = parent;
				}
				parent = parent.GetParent();
			}
			IList<IList<FSAppAttempt>> appsNowMaybeRunnable = new AList<IList<FSAppAttempt>>(
				);
			// Compile lists of apps which may now be runnable
			// We gather lists instead of building a set of all non-runnable apps so
			// that this whole operation can be O(number of queues) instead of
			// O(number of apps)
			if (highestQueueWithAppsNowRunnable != null)
			{
				GatherPossiblyRunnableAppLists(highestQueueWithAppsNowRunnable, appsNowMaybeRunnable
					);
			}
			string user = app.GetUser();
			int userNumRunning = usersNumRunnableApps[user];
			if (userNumRunning == null)
			{
				userNumRunning = 0;
			}
			if (userNumRunning == allocConf.GetUserMaxApps(user) - 1)
			{
				IList<FSAppAttempt> userWaitingApps = usersNonRunnableApps.Get(user);
				if (userWaitingApps != null)
				{
					appsNowMaybeRunnable.AddItem(userWaitingApps);
				}
			}
			UpdateAppsRunnability(appsNowMaybeRunnable, appsNowMaybeRunnable.Count);
		}

		/// <summary>
		/// Checks to see whether applications are runnable now by iterating
		/// through each one of them and check if the queue and user have slack
		/// if we know how many apps can be runnable, there is no need to iterate
		/// through all apps, maxRunnableApps is used to break out of the iteration
		/// </summary>
		private void UpdateAppsRunnability(IList<IList<FSAppAttempt>> appsNowMaybeRunnable
			, int maxRunnableApps)
		{
			// Scan through and check whether this means that any apps are now runnable
			IEnumerator<FSAppAttempt> iter = new MaxRunningAppsEnforcer.MultiListStartTimeIterator
				(appsNowMaybeRunnable);
			FSAppAttempt prev = null;
			IList<FSAppAttempt> noLongerPendingApps = new AList<FSAppAttempt>();
			while (iter.HasNext())
			{
				FSAppAttempt next = iter.Next();
				if (next == prev)
				{
					continue;
				}
				if (CanAppBeRunnable(((FSLeafQueue)next.GetQueue()), next.GetUser()))
				{
					TrackRunnableApp(next);
					FSAppAttempt appSched = next;
					((FSLeafQueue)next.GetQueue()).AddApp(appSched, true);
					noLongerPendingApps.AddItem(appSched);
					if (noLongerPendingApps.Count >= maxRunnableApps)
					{
						break;
					}
				}
				prev = next;
			}
			// We remove the apps from their pending lists afterwards so that we don't
			// pull them out from under the iterator.  If they are not in these lists
			// in the first place, there is a bug.
			foreach (FSAppAttempt appSched_1 in noLongerPendingApps)
			{
				if (!((FSLeafQueue)appSched_1.GetQueue()).RemoveNonRunnableApp(appSched_1))
				{
					Log.Error("Can't make app runnable that does not already exist in queue" + " as non-runnable: "
						 + appSched_1 + ". This should never happen.");
				}
				if (!usersNonRunnableApps.Remove(appSched_1.GetUser(), appSched_1))
				{
					Log.Error("Waiting app " + appSched_1 + " expected to be in " + "usersNonRunnableApps, but was not. This should never happen."
						);
				}
			}
		}

		/// <summary>
		/// Updates the relevant tracking variables after a runnable app with the given
		/// queue and user has been removed.
		/// </summary>
		public virtual void UntrackRunnableApp(FSAppAttempt app)
		{
			// Update usersRunnableApps
			string user = app.GetUser();
			int newUserNumRunning = usersNumRunnableApps[user] - 1;
			if (newUserNumRunning == 0)
			{
				Sharpen.Collections.Remove(usersNumRunnableApps, user);
			}
			else
			{
				usersNumRunnableApps[user] = newUserNumRunning;
			}
			// Update runnable app bookkeeping for queues
			FSLeafQueue queue = ((FSLeafQueue)app.GetQueue());
			FSParentQueue parent = queue.GetParent();
			while (parent != null)
			{
				parent.DecrementRunnableApps();
				parent = parent.GetParent();
			}
		}

		/// <summary>Stops tracking the given non-runnable app</summary>
		public virtual void UntrackNonRunnableApp(FSAppAttempt app)
		{
			usersNonRunnableApps.Remove(app.GetUser(), app);
		}

		/// <summary>
		/// Traverses the queue hierarchy under the given queue to gather all lists
		/// of non-runnable applications.
		/// </summary>
		private void GatherPossiblyRunnableAppLists(FSQueue queue, IList<IList<FSAppAttempt
			>> appLists)
		{
			if (queue.GetNumRunnableApps() < scheduler.GetAllocationConfiguration().GetQueueMaxApps
				(queue.GetName()))
			{
				if (queue is FSLeafQueue)
				{
					appLists.AddItem(((FSLeafQueue)queue).GetCopyOfNonRunnableAppSchedulables());
				}
				else
				{
					foreach (FSQueue child in queue.GetChildQueues())
					{
						GatherPossiblyRunnableAppLists(child, appLists);
					}
				}
			}
		}

		/// <summary>
		/// Takes a list of lists, each of which is ordered by start time, and returns
		/// their elements in order of start time.
		/// </summary>
		/// <remarks>
		/// Takes a list of lists, each of which is ordered by start time, and returns
		/// their elements in order of start time.
		/// We maintain positions in each of the lists.  Each next() call advances
		/// the position in one of the lists.  We maintain a heap that orders lists
		/// by the start time of the app in the current position in that list.
		/// This allows us to pick which list to advance in O(log(num lists)) instead
		/// of O(num lists) time.
		/// </remarks>
		internal class MultiListStartTimeIterator : IEnumerator<FSAppAttempt>
		{
			private IList<FSAppAttempt>[] appLists;

			private int[] curPositionsInAppLists;

			private PriorityQueue<MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime
				> appListsByCurStartTime;

			public MultiListStartTimeIterator(IList<IList<FSAppAttempt>> appListList)
			{
				appLists = Sharpen.Collections.ToArray(appListList, new IList[appListList.Count]);
				curPositionsInAppLists = new int[appLists.Length];
				appListsByCurStartTime = new PriorityQueue<MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime
					>();
				for (int i = 0; i < appLists.Length; i++)
				{
					long time = appLists[i].IsEmpty() ? long.MaxValue : appLists[i][0].GetStartTime();
					appListsByCurStartTime.AddItem(new MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime
						(i, time));
				}
			}

			public override bool HasNext()
			{
				return !appListsByCurStartTime.IsEmpty() && appListsByCurStartTime.Peek().time !=
					 long.MaxValue;
			}

			public override FSAppAttempt Next()
			{
				MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime indexAndTime = appListsByCurStartTime
					.Remove();
				int nextListIndex = indexAndTime.index;
				FSAppAttempt next = appLists[nextListIndex][curPositionsInAppLists[nextListIndex]
					];
				curPositionsInAppLists[nextListIndex]++;
				if (curPositionsInAppLists[nextListIndex] < appLists[nextListIndex].Count)
				{
					indexAndTime.time = appLists[nextListIndex][curPositionsInAppLists[nextListIndex]
						].GetStartTime();
				}
				else
				{
					indexAndTime.time = long.MaxValue;
				}
				appListsByCurStartTime.AddItem(indexAndTime);
				return next;
			}

			public override void Remove()
			{
				throw new NotSupportedException("Remove not supported");
			}

			private class IndexAndTime : Comparable<MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime
				>
			{
				public int index;

				public long time;

				public IndexAndTime(int index, long time)
				{
					this.index = index;
					this.time = time;
				}

				public virtual int CompareTo(MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime
					 o)
				{
					return time < o.time ? -1 : (time > o.time ? 1 : 0);
				}

				public override bool Equals(object o)
				{
					if (!(o is MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime))
					{
						return false;
					}
					MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime other = (MaxRunningAppsEnforcer.MultiListStartTimeIterator.IndexAndTime
						)o;
					return other.time == time;
				}

				public override int GetHashCode()
				{
					return (int)time;
				}
			}
		}
	}
}
