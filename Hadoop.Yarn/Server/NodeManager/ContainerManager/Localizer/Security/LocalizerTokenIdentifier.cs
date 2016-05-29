using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Security
{
	public class LocalizerTokenIdentifier : TokenIdentifier
	{
		public static readonly Text Kind = new Text("Localizer");

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			// TODO Auto-generated method stub
			@out.WriteInt(1);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			// TODO Auto-generated method stub
			@in.ReadInt();
		}

		public override Text GetKind()
		{
			// TODO Auto-generated method stub
			return Kind;
		}

		public override UserGroupInformation GetUser()
		{
			// TODO Auto-generated method stub
			return UserGroupInformation.CreateRemoteUser("testing");
		}
	}
}
