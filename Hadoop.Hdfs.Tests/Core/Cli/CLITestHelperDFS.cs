using Org.Apache.Hadoop.Cli.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class CLITestHelperDFS : CLITestHelper
	{
		protected override CLITestHelper.TestConfigFileParser GetConfigParser()
		{
			return new CLITestHelperDFS.TestConfigFileParserDFS(this);
		}

		internal class TestConfigFileParserDFS : CLITestHelper.TestConfigFileParser
		{
			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void EndElement(string uri, string localName, string qName)
			{
				if (qName.Equals("dfs-admin-command"))
				{
					if (this.testCommands != null)
					{
						this.testCommands.AddItem(new CLITestCmdDFS(this.charString, new CLICommandDFSAdmin
							()));
					}
					else
					{
						if (this.cleanupCommands != null)
						{
							this.cleanupCommands.AddItem(new CLITestCmdDFS(this.charString, new CLICommandDFSAdmin
								()));
						}
					}
				}
				else
				{
					base.EndElement(uri, localName, qName);
				}
			}

			internal TestConfigFileParserDFS(CLITestHelperDFS _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CLITestHelperDFS _enclosing;
		}
	}
}
