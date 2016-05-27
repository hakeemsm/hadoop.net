using System;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Retry
{
	public abstract class UnreliableInterface
	{
		[System.Serializable]
		public class UnreliableException : Exception
		{
			private const long serialVersionUID = 1L;

			private string identifier;

			public UnreliableException()
			{
			}

			public UnreliableException(string identifier)
			{
				// no body
				this.identifier = identifier;
			}

			public override string Message
			{
				get
				{
					return identifier;
				}
			}
		}

		[System.Serializable]
		public class FatalException : UnreliableInterface.UnreliableException
		{
			private const long serialVersionUID = 1L;
			// no body
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract void AlwaysSucceeds();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.FatalException"/>
		public abstract void AlwaysFailsWithFatalException();

		/// <exception cref="Org.Apache.Hadoop.Ipc.RemoteException"/>
		public abstract void AlwaysFailsWithRemoteFatalException();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract void FailsOnceThenSucceeds();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract bool FailsOnceThenSucceedsWithReturnValue();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		public abstract void FailsTenTimesThenSucceeds();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract string SucceedsOnceThenFailsReturningString();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract string SucceedsOnceThenFailsReturningStringIdempotent();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract string SucceedsTenTimesThenFailsReturningString();

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract string FailsIfIdentifierDoesntMatch(string identifier);

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void NonIdempotentVoidFailsIfIdentifierDoesntMatch(string identifier
			);
	}

	public static class UnreliableInterfaceConstants
	{
	}
}
