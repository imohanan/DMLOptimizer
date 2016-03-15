import java.util.List;


public class Fkey {
		private String pk_table;
		private String fk_table;
		private List<String> pk_cols;
		private List<String> fk_cols;
		private String delete_on_cascade;
	
		public Fkey(String pk_table,String fk_table,List<String> pk_cols,List<String> fk_cols,String delete_on_cascade){
			this.pk_table=pk_table;
			this.fk_table=fk_table;
			this.pk_cols=pk_cols;
			this.fk_cols=fk_cols;
			this.delete_on_cascade=delete_on_cascade;
		}
		public String getPk_table() {
			return pk_table;
		}
		public void setPk_table(String pk_table) {
			this.pk_table = pk_table;
		}
		public String getFk_table() {
			return fk_table;
		}
		public void setFk_table(String fk_table) {
			this.fk_table = fk_table;
		}
		public List<String> getPk_cols() {
			return pk_cols;
		}
		public void setPk_cols(List<String> pk_cols) {
			this.pk_cols = pk_cols;
		}
		public List<String> getFk_cols() {
			return fk_cols;
		}
		public void setFk_cols(List<String> fk_cols) {
			this.fk_cols = fk_cols;
		}
		public String getDelete_on_cascade() {
			return delete_on_cascade;
		}
		public void setDelete_on_cascade(String delete_on_cascade) {
			this.delete_on_cascade = delete_on_cascade;
		}
		public String fkToString(){
			String result="ForeignKey_Table:"+this.getFk_table()+" PrimaryKey_Table:"+this.getPk_table()+" ForeignKey_cols:";
			for (String str:this.getFk_cols()){
				result+=str+" ,";
			}
			result=result.substring(0, result.length()-1);
			result+=" PrimaryKey_cols:";
			for (String str:this.getPk_cols()){
				result+=str+" ,";
			}
			result=result.substring(0, result.length()-1);
			result+=" Delete_on_Cascade:"+this.getDelete_on_cascade();
			return result;
		}
					
}
