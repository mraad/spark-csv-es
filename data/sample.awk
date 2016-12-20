BEGIN{
  OFS=","
  XMIN=-180.0
  YMIN=-90.0
  XMAX=180.0
  YMAX=90.0
  DX=XMAX-XMIN
  DY=YMAX-YMIN
  srand()
  print "id,lon,lat,text,nume,real,date"
  for(I=0;I<1000;I++){
    X=XMIN+rand()*DX
    Y=YMIN+rand()*DY

    if(rand() < 0.5)
        TEXT=""
    else
        TEXT="TEXT" rand()

    if(rand() < 0.5)
        NUME=""
    else
        NUME=""+int(10*rand())

    if(rand() < 0.5)
        REAL=""
    else
        REAL=""+rand()

    if(rand() < 0.5)
        DATE=""
    else
        DATE="2000-01-02 12:34:56"

    printf "%d,%.6f,%.6f,%s,%s,%s,%s\n",I,X,Y,TEXT,NUME,REAL,DATE
  }
}
