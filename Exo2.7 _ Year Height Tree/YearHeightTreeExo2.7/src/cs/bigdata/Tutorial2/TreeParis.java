package cs.bigdata.Tutorial2;

public class TreeParis extends Tree{
	
	
	
	public TreeParis(String [] param) {
		super(param);
		this.GEOPOINT = param[0];
		this.ARRONDISSEMENT = param[1];
		this.GENRE = param[2];
		this.ESPECE = param[3];
		this.FAMILLE = param[4];
		this.ANNEE_PLANTATION = param[5];
		this.HAUTEUR = param[6];
		this.CIRCONFERENCE = param[6];
		this.ADRESSE = param[7];
		this.NOM_COMMUN = param[8];
		this.VARIETE = param[9];
		this.OBJECTID = param[10];
		this.NOM_EV = param[11];
	}
	
	 void printTree(Tree tr){
		 System.out.println ("Height:"+ tr.HAUTEUR + "     Year: "+ tr.ANNEE_PLANTATION);		 
	 }
	
	
	
	
	

}
