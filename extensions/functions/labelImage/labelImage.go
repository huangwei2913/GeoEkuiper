// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

/*
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <tensorflow/c/c_api.h>
void SayHello(const char* s) {
	printf("Hello from TensorFlow C library version %s\n", TF_Version());
    puts(s);
}

void NoOpDeallocator(void* data, size_t a, void* b) {}


void DoInference(const char* s, int nbytes){
    TF_Status* status = TF_NewStatus();
    TF_Graph* graph  = TF_NewGraph();
	TF_SessionOptions* session_opts = TF_NewSessionOptions();
	TF_Buffer* run_opts = NULL;
	const char* saved_model_dir = "/home/huangwei/model";
    const char* tags = "serve";
    int ntags = 1;

	TF_Session* session = TF_LoadSessionFromSavedModel(session_opts, run_opts, saved_model_dir, &tags, ntags, graph, NULL, status);
	if (TF_GetCode(status) != TF_OK)
    {
		char results[200];
   		sprintf(results, "\n[ERROR] While loading TensorFlow saved model, got: %s", TF_Message(status));
		puts(results);
        return ;
    }


	printf("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,%d\n",nbytes);

	puts("tensorflow model is loaded......................");

	int NumInputs = 1;
	int NumOutputs = 8;

	TF_Output t_input = {TF_GraphOperationByName(graph, "serving_default_input_tensor"), 0};
	if(t_input.oper == NULL){
		puts("ERROR: Failed TF_GraphOperationByName serving_default_input_tensor\n");
		return ;
	} else{
		puts("TF_GraphOperationByName serving_default_input_tensor is OK\n");
	}


    TF_Output t_output = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 0};
    if (t_output.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}
    else
	{
        puts("TF_GraphOperationByName StatefulPartitionedCall is OK\n");
	}


	TF_Output t_output1 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 1};
    if (t_output1.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}


	TF_Output t_output2 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 2};
    if (t_output2.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}

	TF_Output t_output3 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 3};
    if (t_output3.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}


	TF_Output t_output4 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 4};
    if (t_output4.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}


	TF_Output t_output5 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 5};
    if (t_output5.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}


	TF_Output t_output6 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 6};
    if (t_output6.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}

	TF_Output t_output7 = {TF_GraphOperationByName(graph, "StatefulPartitionedCall"), 7};
    if (t_output7.oper == NULL){
        puts("ERROR: Failed TF_GraphOperationByName StatefulPartitionedCall\n");
		return ;
	}

	TF_Tensor **InputValues = (TF_Tensor **) malloc(sizeof(TF_Tensor *) * NumInputs);
	TF_Tensor **OutputValues = (TF_Tensor **) malloc(sizeof(TF_Tensor *) * NumOutputs);

	int ndims = 4;
	int W=250;
	int H=250;
	int64_t dims[] = {NumInputs,250, 250, 3};
	char data[1 * 250 * 250 * 3];
	int ndata = sizeof(char) * W * H * 3 * NumInputs;
   	memcpy(data, s, nbytes);

	TF_Tensor *int_tensor = TF_NewTensor(TF_UINT8, dims, ndims, data, ndata, &NoOpDeallocator, 0);

	if (int_tensor== NULL)
	{
		printf("ERROR: Failed TF_NewTensor\n");
		return;
	}

	TF_Output *Input = (TF_Output *) malloc(sizeof(TF_Output) * NumInputs);

    TF_Output *Output = (TF_Output *) malloc(sizeof(TF_Output) * NumOutputs);

	Input[0] = t_input;
	Output[0] = t_output;
	Output[1] = t_output1;
	Output[2] = t_output2;
	Output[3] = t_output3;
	Output[4] = t_output4;
	Output[5] = t_output5;
	Output[6] = t_output6;
	Output[7] = t_output7;


	InputValues[0] = int_tensor;

	TF_SessionRun(session, NULL, Input, InputValues, NumInputs, Output, OutputValues, NumOutputs, NULL, 0, NULL,
                  status);


  if (TF_GetCode(status) == TF_OK) {
        printf("Session is OK\n");
    } else {
        printf("%s", TF_Message(status));
		return;
    }

	void* buff = TF_TensorData(OutputValues[1]);
	float* offsets = buff;
	printf("Result Tensor :\n");
	printf("%f\n",offsets[0]);


	void* detection_num = TF_TensorData(OutputValues[5]);
    float* det_num = detection_num;
	printf("The detection num is ...... :\n");
	printf("%f\n",det_num[0]);


	void* dteection_calss= TF_TensorData(OutputValues[2]);
    float* det_class = dteection_calss;
	printf("The detection class is ...... :\n");


	void* dteection_score= TF_TensorData(OutputValues[4]);
    float* det_score = dteection_score;



	for(int i=0;i<100;i++){
		printf("class id is .... dection score...%f,%f\n",det_class[i],det_score[i]*100);
	}




	TF_DeleteSessionOptions(session_opts);
	TF_CloseSession(session, status);
	if (TF_GetCode(status) != TF_OK)
	{
		printf("\n[ERROR] While Close TensorFlow saved model, got: %s", TF_Message(status));
		return ;
	}
	TF_DeleteSession(session, status);
	if (TF_GetCode(status) != TF_OK)
	{
		printf("\n[ERROR] Delete TensorFlow saved model, got: %s", TF_Message(status));
		return ;
	}


	TF_DeleteBuffer(run_opts);
	TF_DeleteGraph(graph);
	TF_DeleteStatus(status);



}

#cgo LDFLAGS: -L/usr/local/lib -ltensorflow
#cgo CFLAGS: -I/usr/local/include/tensorflow

*/
import "C"

import (
	"bufio"
	"bytes"
	"fmt"
	"image"
	_ "image/jpeg"

	"os"
	"sync"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/nfnt/resize"
)

type labelImage struct {
	modelPath string
	labelPath string
	once      sync.Once
	labels    []string
}

func (f *labelImage) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("labelImage function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *labelImage) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	arg0, ok := args[0].([]byte)
	if !ok {
		return fmt.Errorf("labelImage function parameter must be a bytea, but got %[1]T(%[1]v)", args[0]), false
	}

	img, _, err := image.Decode(bytes.NewReader(arg0))

	if err != nil {
		fmt.Println("image decode error............", err.Error())
		return err, false
	}

	resized := resize.Resize(250, 250, img, resize.NearestNeighbor)
	dx := 250
	dy := 250
	bb := make([]byte, 250*250*3)
	for y := 0; y < dy; y++ {
		for x := 0; x < dx; x++ {
			col := resized.At(x, y)
			r, g, b, _ := col.RGBA()
			bb[(y*dx+x)*3+0] = byte(float64(r) / 255.0)
			bb[(y*dx+x)*3+1] = byte(float64(g) / 255.0)
			bb[(y*dx+x)*3+2] = byte(float64(b) / 255.0)
		}
	}

	fmt.Println("huangwei........................xxxxh....")

	//f.once.Do(func() {
	myString := string(bb[:])
	C.DoInference(C.CString(myString), C.int(len(myString)))
	// convert byte[] to string
	// cs := C.CBytes(bb) // 在 C 空间分配内存
	// defer C.free(unsafe.Pointer(cs))
	//C.DoInference(cs, C.int(len(bb)))
	//})

	//change byte to unsafe pointers

	return "huangwei......................", true
}

func (f *labelImage) IsAggregate() bool {
	return false
}

func loadLabels(filename string) ([]string, error) {
	labels := []string{}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		labels = append(labels, scanner.Text())
	}
	return labels, nil
}

var LabelImage = labelImage{
	modelPath: "labelImage/mobilenet_quant_v1_224.tflite",
	labelPath: "labelImage/labels.txt",
}
