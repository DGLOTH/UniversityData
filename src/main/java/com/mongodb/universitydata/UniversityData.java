package com.mongodb.universitydata;

import com.bryanreinero.firehose.Converter;
import com.bryanreinero.firehose.Transformer;
import com.bryanreinero.firehose.cli.CallBack;
import com.bryanreinero.firehose.metrics.Interval;
import com.bryanreinero.firehose.metrics.SampleSet;

import com.bryanreinero.util.Application;
import com.bryanreinero.util.WorkerPool.Executor;
import com.bryanreinero.util.WorkerPool;
import com.bryanreinero.util.DAO;
import com.bryanreinero.util.Printer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import java.text.SimpleDateFormat;
import java.lang.Object;
import org.bson.types.ObjectId;

public class UniversityData implements Executor {
	
	private static final String appName = "UniversityData";
	private final Application worker;
	private final SampleSet samples;

    private String dbname = "university";
    private Integer maxCount = 0;
    private String contentSubDir = "small";
	private AtomicInteger count = new AtomicInteger(0);

	private AtomicInteger countUniversity = new AtomicInteger(0);
	private AtomicInteger countProfessors = new AtomicInteger(0);
	private AtomicInteger countStudents = new AtomicInteger(0);
	private AtomicInteger countCourses = new AtomicInteger(0);
	private AtomicInteger countSubmissions = new AtomicInteger(0);

    private UniversityDataCallBack universityCB = new UniversityDataCallBack();
    private UniversityDataCallBack coursesCB = new UniversityDataCallBack();
    private UniversityDataCallBack cityCB = new UniversityDataCallBack();
    private UniversityDataCallBack streetsCB = new UniversityDataCallBack();
    private UniversityDataCallBack lastCB = new UniversityDataCallBack();
    private UniversityDataCallBack firstCB = new UniversityDataCallBack();

	private Converter converter = new Converter();
	private DAO daoUniversity = null;
	private DAO daoProfessors = null;
	private DAO daoStudents = null;
	private DAO daoCourses = null;
	private DAO daoSubmissions = null;
    private DAO daoContent = null;
	
	private static Boolean verbose = false;
	private String filename = null;
	
    class UniversityDataCallBack implements CallBack {
        
        private String filename = null;
        private BufferedReader br = null;
        private Converter converter = new Converter();
        private Integer linesRead = 0;
        public List<DBObject> DBObjects = null;
        
        public void dataFileHandler(String type, String[] values) {
            filename  = values[0];
            try {
                BufferedReader br = null;
                if (type.equals("resource")) {
                    InputStream is =
                        UniversityData.class.getResourceAsStream(filename);
                    br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                } else if (type.equals("filesystem")) {
                    br = new BufferedReader(new FileReader(filename));
                }

                DBObjects = new ArrayList<DBObject>();
                
                // read header line from file
                String ln = br.readLine();
                String colDelim = ln.substring(0,1);
                String fieldDelim = ln.substring(1,2);
                String header = ln.substring(2);
                converter.setDelimiter( colDelim.charAt(0) );
                for (String column : header.split(colDelim)) {
                    String[] s = column.split(fieldDelim);
                    converter.addField(s[0], Transformer.getTransformer(s[1]));
                }
                
                // read data lines
                try {
                    
                    // read the next line from source file
                    while ((ln=br.readLine()) != null) {

                        linesRead += 1;
                        
                        // Create the university DBObject
                        DBObject object = converter.convert( ln );
                        
                        // Add the DBObject to the DBObjects array
                        DBObjects.add(object);
                    }
                } catch (IOException e) {
                    System.out.println
                        ("Caught exception in UniversityDataCallBack: "
                         +e.getMessage());
                    if (verbose)
                        e.printStackTrace();
                    try {
                        synchronized ( br ) {
                            if (br != null) br.close();
                        }
                    } catch (IOException ex) {
                        System.out.println
                            ("Caught exception in UniversityDataCallBack: "
                             +e.getMessage());
                        if (verbose)
                            ex.printStackTrace();
                    }
                }
                br.close();
                br = null;
            } catch (Exception e) {
                System.out.println("Caught excepting in UniversityDataCallback: "
                                   +e.getMessage());
                if (verbose)
                    e.printStackTrace();
                System.exit(-1);
            }
        }

        public void handle(String[] values) {
            dataFileHandler ("filesystem", values);
        }
    }
    
	public UniversityData ( String[] args ) throws Exception {
		
		Map<String, CallBack> myCallBacks = new HashMap<String, CallBack>();
		
		//  command line callback for dbname
		myCallBacks.put("db", new CallBack() {
			@Override
			public void handle(String[] values) {
                dbname = values[0];
			}
		});

		// custom command line callback for count
		myCallBacks.put("c", new CallBack() {
			@Override
			public void handle(String[] values) {
                maxCount = Integer.parseInt(values[0]);
			}
		});

        // Content directory; defaults to 'small' w/in resources directory
		myCallBacks.put("cd", new CallBack() {
			@Override
			public void handle(String[] values) {
				contentSubDir = values[0];
			}

		});

        // Verbose
		myCallBacks.put("v", new CallBack() {
			@Override
			public void handle(String[] values) {
				verbose = true;
			}
		});

		// custom command line callback for university data file
		myCallBacks.put("fh", universityCB);

		// custom command line callback for courses data file
		myCallBacks.put("fp", coursesCB);

		// custom command line callback for city data file
		myCallBacks.put("fc", cityCB);

		// custom command line callback for streets data file
		myCallBacks.put("fs", streetsCB);

		// custom command line callback for last data file
		myCallBacks.put("fl", lastCB);

		// custom command line callback for first data file
		myCallBacks.put("ff", firstCB);

		worker = Application.ApplicationFactory.getApplication
            (appName, this, args, myCallBacks);

        if (universityCB.DBObjects == null)
            universityCB.dataFileHandler
                ("resource", new String[] {"/data/university.dat"});
        if (coursesCB.DBObjects == null)
            coursesCB.dataFileHandler
                ("resource", new String[] {"/data/courses.dat"});
        if (cityCB.DBObjects == null)
            cityCB.dataFileHandler
                ("resource", new String[] {"/data/city.dat"});
        if (streetsCB.DBObjects == null)
            streetsCB.dataFileHandler
                ("resource", new String[] {"/data/streets.dat"});
        if (lastCB.DBObjects == null)
            lastCB.dataFileHandler
                ("resource", new String[] {"/data/last.dat"});
        if (firstCB.DBObjects == null)
            firstCB.dataFileHandler
                ("resource", new String[] {"/data/first.dat"});

		samples = worker.getSampleSet();
        daoUniversity = worker.getDAO(dbname, "university");
        daoProfessors = worker.getDAO(dbname, "professors");
        daoStudents = worker.getDAO(dbname, "students");
        daoCourses = worker.getDAO(dbname, "courses");
        BasicDBObject coursesIndex = new BasicDBObject()
            .append("university", 1)
            .append("professor", 1)
            .append("student", 1)
            .append("type", 1)
            ;
        daoCourses.createIndex(coursesIndex);
        daoSubmissions = worker.getDAO(dbname, "submissions");
        daoContent = worker.getDAO(dbname, "content");
		worker.addPrintable(this);
		worker.start();

	}
	
    @Override
    public void execute() {

        try {

            Integer currentCount = count.incrementAndGet();

            Interval intTotal = samples.set("Total");
            if (currentCount > maxCount) {
                worker.stop();

            } else {

                Random rnd = new Random();

                // Generate some random data
                // university, professor, student, course, submission

                // Various IDs
                int universityId = rnd.nextInt(universityCB.linesRead);
                int professorId = rnd.nextInt(50000);   // 50k US doctors
                int studentId = rnd.nextInt(300000000); // 300M US pop.; ssn
                int courseIdx = rnd.nextInt(coursesCB.linesRead);
                ObjectId courseId = ObjectId.get();
                ObjectId submissionId = ObjectId.get();

                // Submission
                Interval intSubmissionTotal = samples.set("SubmissionTotal");
                Interval intSubmissionBuild = samples.set("SubmissionBuild");
                String[] submissionTypes = {
                    "txt",   "txt",   "txt",   "txt", // 40% txt (1k)
                    "jpg", "jpg",                     // 20% jpg (200k)
                    "pdf", "pdf",                     // 20% pdf (500k)
                    "pdf",                            // 10% pdf (1M)
                    "pdf"                             // 10% pdf (13M)
                };
                String[] submissionFiles = {
                    "data-1k.txt", "data-1k.txt", "data-1k.txt", "data-1k.txt",
                    "data-200k.jpg", "data-200k.jpg",
                    "data-500k.pdf", "data-500k.pdf",
                    "data-1M.pdf",
                    "data-13M.pdf"
                };
                int submissionTypeIdx = rnd.nextInt(submissionTypes.length);
                // Resource directory based content file
                String submissionFile =
                    "/content/"+contentSubDir+"/"+submissionFiles[submissionTypeIdx];
                InputStream submissionFileIS =
                    UniversityData.class.getResourceAsStream(submissionFile);
                DataInputStream submissionFileDIS =
                    new DataInputStream(submissionFileIS);
                int submissionFileLength = submissionFileIS.available();
                byte[] submissionFileData = new byte[submissionFileLength];
                submissionFileDIS.readFully(submissionFileData);
                submissionFileDIS.close();
                submissionFileIS.close();
                BasicDBObject submissionDoc = new BasicDBObject()
                    .append("_id", submissionId)
                    .append("type", submissionTypes[submissionTypeIdx])
                    .append("size", submissionFileLength)
                    .append("content", submissionFileData)
                    .append("course", courseId);
                    ;
                intSubmissionBuild.mark();
                WriteResult submissionResult = null;
                try {
                    Interval intSubmissionInsert = samples.set("SubmissionInsert");
                    submissionResult = daoSubmissions.insert (submissionDoc);
                    intSubmissionInsert.mark();
                    countSubmissions.incrementAndGet();
                } catch (Exception e) {
                    System.out.println ("Caught exception in SubmissionInsert: "
                                        +e.getMessage());
                    System.out.println ("submission: "+submissionDoc);
                }
                intSubmissionTotal.mark();

                // Course
                Interval intCourseTotal = samples.set("CourseTotal");
                Interval intCourseBuild = samples.set("CourseBuild");
                String courseType =
                    (String)coursesCB.DBObjects.get(courseIdx).get("name");
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd");
                String dateInString = String.format
                    ( "%4d.%02d.%02d",rnd.nextInt(5)+2000,rnd.nextInt(11)+1,
                      rnd.nextInt(28));
                Date courseDate = formatter.parse(dateInString);
                BasicDBObject courseQuery = new BasicDBObject()
                    .append("_id", courseId)
                    .append("university", universityId)
                    .append("professor", professorId)
                    .append("student", studentId)
                    .append("type", courseType)
                    ;
                BasicDBObject courseDoc = new BasicDBObject()
                    .append("$setOnInsert", courseQuery)
                    .append("$addToSet", new BasicDBObject("submissions", submissionId))
                    ;
                intCourseBuild.mark();
                WriteResult courseResult = null;
                try {
                    Interval intCourseInsert = samples.set("CourseInsert");
                    courseResult =
                        daoCourses.update (courseQuery, courseDoc,
                                              true, false);
                    intCourseInsert.mark();
                    countCourses.incrementAndGet();
                } catch (Exception e) {
                    System.out.println ("Caught exception in CourseUpdate: "
                                        +e.getMessage());
                    System.out.println ("course: "+courseDoc);
                }
                intCourseTotal.mark();

                // Student
                Interval intStudentTotal = samples.set("StudentTotal");
                Interval intStudentBuild = samples.set("StudentBuild");
                String studentFirst = (String)firstCB.DBObjects.get
                    (rnd.nextInt(firstCB.linesRead)).get("name");
                String studentLast = (String)lastCB.DBObjects.get
                    (rnd.nextInt (lastCB.linesRead)).get("name");
                String studentStreet =
                    Integer.toString(rnd.nextInt(1000))+" "+
                    (String)streetsCB.DBObjects
                    .get(rnd.nextInt(streetsCB.linesRead)).get("name");
                int cityIdx = rnd.nextInt(cityCB.linesRead);
                String studentCity = (String)cityCB.DBObjects.get
                    (cityIdx).get("city");
                String studentState = (String)cityCB.DBObjects.get
                    (cityIdx).get("state");
                Integer studentZip = (Integer)cityCB.DBObjects.get
                    (cityIdx).get("zip");
                BasicDBObject studentAddr =
                    new BasicDBObject()
                    .append("street", studentStreet)
                    .append("city", studentCity)
                    .append("state", studentState)
                    .append("zip", studentZip);
                BasicDBObject studentQuery = new BasicDBObject()
                    .append("_id", studentId)
                    ;
                BasicDBObject studentDocFields = new BasicDBObject()
                    .append("_id", studentId)
                    .append("first", studentFirst)
                    .append("last", studentLast)
                    .append("addr", studentAddr)
                    ;
                BasicDBObject studentDocPush = new BasicDBObject()
                    .append("professors", professorId)
                    .append("courses", courseResult.getUpsertedId())
                    ;
                BasicDBObject studentDoc = new BasicDBObject()
                    .append("$setOnInsert", studentDocFields)
                    .append("$addToSet", studentDocPush)
                    ;
                intStudentBuild.mark();
                WriteResult studentResult = null;
                try {
                    Interval intStudentUpdate = samples.set("StudentUpdate");
                    studentResult =
                        daoStudents.update (studentQuery, studentDoc,
                                            true, false);
                    intStudentUpdate.mark();
                    countStudents.incrementAndGet();
                } catch (Exception e) {
                    System.out.println ("Caught exception in StudentUpdate: "
                                        +e.getMessage());
                    System.out.println ("student: "+studentDoc);
                }
                intStudentTotal.mark();

                // Professor
                Interval intProfessorTotal = samples.set("ProfessorTotal");
                Interval intProfessorBuild = samples.set("ProfessorBuild");
                String professorFirst = (String)firstCB.DBObjects.get
                    (rnd.nextInt(firstCB.linesRead)).get("name");
                String professorLast = (String)lastCB.DBObjects.get
                    (rnd.nextInt (lastCB.linesRead)).get("name");
                String professorStreet =
                    Integer.toString(rnd.nextInt(1000))+" "+
                    (String)streetsCB.DBObjects
                    .get(rnd.nextInt(streetsCB.linesRead)).get("name");
                cityIdx = rnd.nextInt(cityCB.linesRead);
                String professorCity = (String)cityCB.DBObjects.get
                    (cityIdx).get("city");
                String professorState = (String)cityCB.DBObjects.get
                    (cityIdx).get("state");
                Integer professorZip = (Integer)cityCB.DBObjects.get
                    (cityIdx).get("zip");
                BasicDBObject professorAddr =
                    new BasicDBObject()
                    .append("street", professorStreet)
                    .append("city", professorCity)
                    .append("state", professorState)
                    .append("zip", professorZip);
                BasicDBObject professorQuery = new BasicDBObject()
                    .append("_id", professorId)
                    ;
                BasicDBObject professorDocFields = new BasicDBObject()
                    .append("_id", professorId)
                    .append("first", professorFirst)
                    .append("last", professorLast)
                    .append("addr", professorAddr)
                    ;
                BasicDBObject professorDoc = new BasicDBObject()
                    .append("$setOnInsert", professorDocFields)
                    .append("$addToSet", new BasicDBObject
                            ("university", universityId))
                    ;
                intProfessorBuild.mark();
                WriteResult professorResult = null;
                try {
                    Interval intProfessorUpdate = samples.set("ProfessorUpdate");
                    professorResult =
                        daoProfessors.update (professorQuery, professorDoc,
                                            true, false);
                    intProfessorUpdate.mark();
                    countProfessors.incrementAndGet();
                } catch (Exception e) {
                    System.out.println ("Caught exception in ProfessorUpdate: "
                                        +e.getMessage());
                    System.out.println ("professor: "+professorDoc);
                }
                intProfessorTotal.mark();

                // University
                Interval intUniversityTotal = samples.set("UniversityTotal");
                Interval intUniversityBuild = samples.set("UniversityBuild");
                int universityIdx = rnd.nextInt(universityCB.linesRead);
                String universityName = (String)universityCB.DBObjects.get
                    (universityIdx).get("name");
                String universityCity = (String)universityCB.DBObjects.get
                    (universityIdx).get("city");
                String universityState = (String)universityCB.DBObjects.get
                    (universityIdx).get("state");
                int universityBeds = rnd.nextInt(280) + 20; // min 20; max 300
                Boolean[] universityBooleans = {true,false};
                Boolean universityTraumaCenter = universityBooleans[rnd.nextInt(2)];
                BasicDBObject universityQuery = new BasicDBObject()
                    .append("_id", universityId)
                    ;
                BasicDBObject universityDocFields = new BasicDBObject()
                    .append("_id", universityId)
                    .append("name", universityName)
                    .append("city", universityCity)
                    .append("state", universityState)
                    .append("beds", universityBeds)
                    .append("trauma center", universityTraumaCenter)
                    ;
                BasicDBObject universityDoc = new BasicDBObject()
                    .append("$setOnInsert", universityDocFields)
                    .append("$addToSet", new BasicDBObject
                            ("professors", professorId))
                    ;
                intUniversityBuild.mark();
                WriteResult universityResult = null;
                try {
                    Interval intUniversityUpdate = samples.set("UniversityUpdate");
                    universityResult =
                        daoUniversity.update (universityQuery, universityDoc,
                                            true, false);
                    intUniversityUpdate.mark();
                    countUniversity.incrementAndGet();
                } catch (Exception e) {
                    System.out.println ("Caught exception in UniversityUpdate: "
                                        +e.getMessage());
                    System.out.println ("university: "+universityDoc);
                }
                intUniversityTotal.mark();

            }
            intTotal.mark();

        } catch (Exception e) {
            System.out.println("Caught exception in execute:"+e.getMessage());
            e.printStackTrace();
            worker.stop();
        }
    }
	
	@Override 
	public String toString() {
		StringBuffer buf = new StringBuffer("{ ");
		buf.append(String.format("threads: "+worker.getNumThreads()));
		buf.append(String.format(", count: "+this.count ));
		buf.append(String.format(", countUniversity: "+this.countUniversity ));
		buf.append(String.format(", countProfessors: "+this.countProfessors ));
		buf.append(String.format(", countStudents: "+this.countStudents ));
		buf.append(String.format(", countCourses: "+this.countCourses ));
		buf.append(String.format(", countSubmissions: "+this.countSubmissions ));
		buf.append(String.format(", samples: "+ samples ));
		
		if( verbose ) {
			buf.append(", dao: "+daoUniversity);
			buf.append(", dao: "+daoProfessors);
			buf.append(", dao: "+daoStudents);
			buf.append(", dao: "+daoCourses);
			buf.append(", dao: "+daoSubmissions);
		}
		buf.append(" }");
		return buf.toString();
	}
    
    public static void main( String[] args ) {
    	
    	try {
    		UniversityData uni = new UniversityData( args );
		} 
		catch (Exception e) {
            System.out.println("Caught exception in main:"+e.getMessage());
            if (verbose)
                e.printStackTrace();
            System.out.println("Exiting from main.");
			System.exit(-1);
		}
    }
}
