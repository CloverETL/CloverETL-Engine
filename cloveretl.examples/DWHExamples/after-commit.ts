<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="DWH-examples" description="Engine advanced examples" useJMX="true">

    <FunctionalTest ident="D_DATE" graphFile="graph/D_DATE.grf">
        <FlatFile outputFile="tables/D_DATE.tbl" supposedFile="supposed-out/D_DATE.tbl"/>
         <DeleteFile file="seq/ID_D_DATE.seq"/>
    </FunctionalTest>
	
    <FunctionalTest ident="D_STORE_SCD1" graphFile="graph/D_STORE_SCD1.grf">
        <FlatFile outputFile="tables/D_STORE_update.tbl" supposedFile="supposed-out/D_STORE_update.tbl"/>
        <FlatFile outputFile="tables/D_STORE_insert.tbl" supposedFile="supposed-out/D_STORE_insert.tbl"/>
         <DeleteFile file="seq/ID_D_STORE.seq"/>
    </FunctionalTest>
	
    <FunctionalTest ident="D_CUSTOMER_SCD2" graphFile="graph/D_CUSTOMER_SCD2.grf">
    <!-- Can't test as we use today() function
        <FlatFile outputFile="tables/D_CUSTOMER_update.tbl" supposedFile="supposed-out/D_CUSTOMER_update.tbl"/>
        <FlatFile outputFile="tables/D_CUSTOMER_insert.tbl" supposedFile="supposed-out/D_CUSTOMER_insert.tbl"/>
    -->
         <DeleteFile file="seq/ID_D_CUSTOMER.seq"/>
    </FunctionalTest>
	
    <FunctionalTest ident="D_PRODUCT_SCD3" graphFile="graph/D_PRODUCT_SCD3.grf">
        <FlatFile outputFile="tables/D_PRODUCT_update.tbl" supposedFile="supposed-out/D_PRODUCT_update.tbl"/>
        <FlatFile outputFile="tables/D_PRODUCT_insert.tbl" supposedFile="supposed-out/D_PRODUCT_insert.tbl"/>
         <DeleteFile file="seq/ID_D_PRODUCT.seq"/>
    </FunctionalTest>
	
    <FunctionalTest ident="F_SALES" graphFile="graph/F_SALES.grf">
        <FlatFile outputFile="tables/F_SALES.tbl" supposedFile="supposed-out/F_SALES.tbl"/>
    </FunctionalTest>
	
</TestScenario>
