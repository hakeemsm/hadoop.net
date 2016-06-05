using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class InfoBlock : HtmlBlock
	{
		internal readonly ResponseInfo info;

		[Com.Google.Inject.Inject]
		internal InfoBlock(ResponseInfo info)
		{
			this.info = info;
		}

		protected internal override void Render(HtmlBlock.Block html)
		{
			Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> table = html
				.Div(JQueryUI.InfoWrap).Table(JQueryUI.Info).Tr().Th().$class(JQueryUI.CTh).$colspan
				(2).(info.About()).().();
			int i = 0;
			foreach (ResponseInfo.Item item in info)
			{
				Hamlet.TR<Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>> 
					tr = table.Tr((++i % 2 != 0) ? JQueryUI.Odd : JQueryUI.Even).Th(item.key);
				string value = item.value.ToString();
				if (item.url == null)
				{
					if (!item.isRaw)
					{
						Hamlet.TD<Hamlet.TR<Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
							>>>> td = tr.Td();
						if (value.LastIndexOf('\n') > 0)
						{
							string[] lines = value.Split("\n");
							Hamlet.DIV<Hamlet.TD<Hamlet.TR<Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
								>>>>> singleLineDiv;
							foreach (string line in lines)
							{
								singleLineDiv = td.Div();
								singleLineDiv.(line);
								singleLineDiv.();
							}
						}
						else
						{
							td.(value);
						}
						td.();
					}
					else
					{
						tr.Td()._r(value).();
					}
				}
				else
				{
					tr.Td().A(Url(item.url), value).();
				}
				tr.();
			}
			table.().();
		}
	}
}
