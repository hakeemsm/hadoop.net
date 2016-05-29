using System.Collections.Generic;
using System.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class MockContainerLocalizer
	{
		public static void BuildMainArgs(IList<string> command, string user, string appId
			, string locId, IPEndPoint nmAddr, IList<string> localDirs)
		{
			command.AddItem(typeof(MockContainerLocalizer).FullName);
			command.AddItem(user);
			command.AddItem(appId);
			command.AddItem(locId);
			command.AddItem(nmAddr.GetHostName());
			command.AddItem(Sharpen.Extensions.ToString(nmAddr.Port));
			foreach (string dir in localDirs)
			{
				command.AddItem(dir);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
		}
		//DO Nothing
	}
}
