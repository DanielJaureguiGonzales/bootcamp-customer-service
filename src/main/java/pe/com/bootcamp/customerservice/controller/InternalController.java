package pe.com.bootcamp.customerservice.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import pe.com.bootcamp.customerservice.dto.CustomerResponse;
import pe.com.bootcamp.customerservice.dto.CustomerSummaryResponse;
import pe.com.bootcamp.customerservice.dto.DocumentNumbersRequest;
import pe.com.bootcamp.customerservice.service.CustomerService;
import reactor.core.publisher.Mono;

import java.util.List;

@RequestMapping("/api/internal/customers")
@RestController
@RequiredArgsConstructor
public class InternalController {

    private final CustomerService customerService;

    @PostMapping("/document-numbers/search")
    public Mono<List<CustomerSummaryResponse>> searchCustomerReponse(@RequestBody @Valid DocumentNumbersRequest documentNumbersRequests) {
        return customerService.findByDocumentNumbers(documentNumbersRequests);

    }

    @GetMapping("/document-number")
    public Mono<CustomerResponse> getCustomerResponse(
            @RequestParam String documentType,
            @RequestParam String documentNumber
    ){
        return customerService.findByDocumentNumberAndDocumentType( documentNumber, documentType);
    }
}
