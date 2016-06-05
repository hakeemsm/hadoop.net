using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// FileBasedIPList loads a list of subnets in CIDR format and ip addresses from
	/// a file.
	/// </summary>
	/// <remarks>
	/// FileBasedIPList loads a list of subnets in CIDR format and ip addresses from
	/// a file.
	/// Given an ip address, isIn  method returns true if ip belongs to one of the
	/// subnets.
	/// Thread safe.
	/// </remarks>
	public class FileBasedIPList : IPList
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.FileBasedIPList
			));

		private readonly string fileName;

		private readonly MachineList addressList;

		public FileBasedIPList(string fileName)
		{
			this.fileName = fileName;
			string[] lines;
			try
			{
				lines = ReadLines(fileName);
			}
			catch (IOException)
			{
				lines = null;
			}
			if (lines != null)
			{
				addressList = new MachineList(new HashSet<string>(Arrays.AsList(lines)));
			}
			else
			{
				addressList = null;
			}
		}

		public virtual Org.Apache.Hadoop.Util.FileBasedIPList Reload()
		{
			return new Org.Apache.Hadoop.Util.FileBasedIPList(fileName);
		}

		public virtual bool IsIn(string ipAddress)
		{
			if (ipAddress == null || addressList == null)
			{
				return false;
			}
			return addressList.Includes(ipAddress);
		}

		/// <summary>Reads the lines in a file.</summary>
		/// <param name="fileName"/>
		/// <returns>
		/// lines in a String array; null if the file does not exist or if the
		/// file name is null
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private static string[] ReadLines(string fileName)
		{
			try
			{
				if (fileName != null)
				{
					FilePath file = new FilePath(fileName);
					if (file.Exists())
					{
						using (StreamReader fileReader = new InputStreamReader(new FileInputStream(file), 
							Charsets.Utf8))
						{
							using (BufferedReader bufferedReader = new BufferedReader(fileReader))
							{
								IList<string> lines = new AList<string>();
								string line = null;
								while ((line = bufferedReader.ReadLine()) != null)
								{
									lines.AddItem(line);
								}
								if (Log.IsDebugEnabled())
								{
									Log.Debug("Loaded IP list of size = " + lines.Count + " from file = " + fileName);
								}
								return (Collections.ToArray(lines, new string[lines.Count]));
							}
						}
					}
					else
					{
						Log.Debug("Missing ip list file : " + fileName);
					}
				}
			}
			catch (IOException ioe)
			{
				Log.Error(ioe);
				throw;
			}
			return null;
		}
	}
}
