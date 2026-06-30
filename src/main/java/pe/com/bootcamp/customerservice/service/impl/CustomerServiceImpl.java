package pe.com.bootcamp.customerservice.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import pe.com.bootcamp.customerservice.dto.CustomerRequest;
import pe.com.bootcamp.customerservice.dto.CustomerResponse;
import pe.com.bootcamp.customerservice.dto.CustomerSummaryResponse;
import pe.com.bootcamp.customerservice.dto.DocumentNumbersRequest;
import pe.com.bootcamp.customerservice.exceptions.AlreadyCustomerExistsException;
import pe.com.bootcamp.customerservice.exceptions.CustomersParticipantsException;
import pe.com.bootcamp.customerservice.exceptions.ResourceNotFoundException;
import pe.com.bootcamp.customerservice.mapper.CustomerMapper;
import pe.com.bootcamp.customerservice.model.Customer;
import pe.com.bootcamp.customerservice.repository.CustomerRepository;
import pe.com.bootcamp.customerservice.service.CustomerService;
import pe.com.bootcamp.customerservice.strategy.CustomerValidationStrategy;
import pe.com.bootcamp.customerservice.strategy.factory.CustomerValidationStrategyFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerServiceImpl implements CustomerService {

    private final CustomerRepository customerRepository;
    private final CustomerMapper customerMapper;
    private final CustomerValidationStrategyFactory validationStrategyFactory;

    @Override
    public Flux<CustomerResponse> findAll() {
        log.info("Starting active customers search");
        return customerRepository.findAllByStatus(true)
                .map(customerMapper::toCustomerResponse)
                .doOnComplete(() ->
                        log.info("Active customers search completed successfully")
                )
                .doOnError(error ->
                        logUnexpectedError("Error searching active customers", error)
                );
    }

    @Override
    public Mono<CustomerResponse> findById(String id) {
        log.info("Searching customer by id={}", id);
        return customerRepository.findByIdAndStatus(id,true)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Customer", "Id", id)))
                .map(customerMapper::toCustomerResponse)
                .doOnSuccess(response ->
                        log.info("Customer found successfully. customerId={}", response.id())
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn("Customer not found by id={}", id)
                )
                .doOnError(error ->
                        logUnexpectedError("Unexpected error searching customer by id=" + id, error)
                );
    }

    @Override
    public Mono<CustomerResponse> findByDocumentNumber(String documentNumber) {
        log.info("Searching customer by documentNumber={}", maskDocument(documentNumber));


        return customerRepository.findByDocumentNumberAndStatus(documentNumber,true)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Customer", "documentNumber", documentNumber)))
                .map(customerMapper::toCustomerResponse)
                .doOnSuccess(response ->
                        log.info(
                                "Customer found successfully by documentNumber. customerId={}",
                                response.id()
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Customer not found by documentNumber={}",
                                maskDocument(documentNumber)
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error searching customer by documentNumber="
                                        + maskDocument(documentNumber),
                                error
                        )
                );
    }

    @Override
    public Mono<CustomerResponse> createCustomer(CustomerRequest customerRequest) {

        log.info(
                "Starting customer creation. documentType={}, documentNumber={}",
                customerRequest.documentType(),
                maskDocument(customerRequest.documentNumber())
        );

        CustomerValidationStrategy strategy = validationStrategyFactory.getStrategy(customerRequest.documentType());

        return strategy.validate(customerRequest)
                .doOnSuccess(unused ->
                        log.debug("Customer request validation completed.")
                )
                .then(customerRepository.existsByDocumentNumberAndStatus(customerRequest.documentNumber(), true))
                .flatMap(exists -> {

                    if (Boolean.TRUE.equals(exists)) {
                        return Mono.error(new AlreadyCustomerExistsException(String.format("Customer already " +
                                "exists with %s: %s", "documentNumber", customerRequest.documentNumber())));
                    }
                    Customer customer = customerMapper.toCustomer(customerRequest);
                    customer.setRegistrationDate(LocalDateTime.now());
                    customer.setStatus(true);

                    return customerRepository.save(customer);
                })
                .map(customerMapper::toCustomerResponse)
                .doOnSuccess(response ->
                        log.info(
                                "Customer created successfully. customerId={}, documentType={}, documentNumber={}",
                                response.id(),
                                response.documentType(),
                                maskDocument(response.documentNumber())
                        )
                )
                .doOnError(AlreadyCustomerExistsException.class, error ->
                        log.warn(
                                "Customer creation rejected because customer already exists. documentType={}, documentNumber={}",
                                customerRequest.documentType(),
                                maskDocument(customerRequest.documentNumber())
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error creating customer. documentType="
                                        + customerRequest.documentType()
                                        + ", documentNumber="
                                        + maskDocument(customerRequest.documentNumber()),
                                error
                        )
                );

    }

    @Override
    public Mono<CustomerResponse> updateCustomer(String id, CustomerRequest customerRequest) {

        log.info(
                "Starting customer update. customerId={}, documentType={}, documentNumber={}",
                id,
                customerRequest.documentType(),
                maskDocument(customerRequest.documentNumber())
        );

        return customerRepository.findByIdAndStatus(id,true)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Customer", "Id", id)))
                .flatMap(customer -> {
                    customerMapper.updateCustomer(customer, customerRequest);
                    return customerRepository.save(customer);
                })
                .map(customerMapper::toCustomerResponse)
                .doOnSuccess(response ->
                        log.info(
                                "Customer updated successfully. customerId={}, documentType={}, documentNumber={}",
                                response.id(),
                                response.documentType(),
                                maskDocument(response.documentNumber())
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn("Customer update rejected because customer was not found. customerId={}", id)
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error updating customer. customerId=" + id,
                                error
                        )
                );
    }

    @Override
    public Mono<Void> deleteCustomer(String id) {
        log.info("Starting logical customer deletion. customerId={}", id);
        return customerRepository.findByIdAndStatus(id,true)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Customer", "Id", id)))
                .flatMap(customer -> {
                                customer.setStatus(false);
                                return customerRepository.save(customer);
                            }
                        )
                .doOnSuccess(customer ->
                        log.info("Customer logically deleted successfully. customerId={}", id)
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn("Customer deletion rejected because customer was not found. customerId={}", id)
                )
                .doOnError(error ->logUnexpectedError(
                        "Unexpected error deleting customer. customerId=" + id,
                        error
                ))
                .then();
    }

    @Override
    public Mono<CustomerResponse> findByDocumentNumberAndDocumentType(String documentNumber, String documentType) {

        log.info(
                "Searching customer by documentType and documentNumber. documentType={}, documentNumber={}",
                documentType,
                maskDocument(documentNumber)
        );

        return customerRepository.findByDocumentNumberAndDocumentTypeAndStatus(documentNumber, documentType, true)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                        "Customer",
                        "documentNumber",
                        documentNumber
                )))
                .map(customerMapper::toCustomerResponse)
                .doOnSuccess(response ->
                        log.info(
                                "Customer found by documentType and documentNumber. customerId={}",
                                response.id()
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Customer not found by documentType and documentNumber. documentType={}, documentNumber={}",
                                documentType,
                                maskDocument(documentNumber)
                        )
                )
                .doOnError(error ->logUnexpectedError(
                        "Unexpected error searching customer by documentType="
                                + documentType
                                + ", documentNumber="
                                + maskDocument(documentNumber),
                        error
                ));
    }

    @Override
    public Mono<List<CustomerSummaryResponse>> findByDocumentNumbers(DocumentNumbersRequest documentNumbersRequests) {

        List<String> uniqueDocumentNumbers = documentNumbersRequests.documentNumbers().stream()
                .map(String::trim)
                .distinct()
                .toList();

        log.info(
                "Starting customers search by document number list. requestedCount={}, uniqueCount={}",
                documentNumbersRequests.documentNumbers().size(),
                uniqueDocumentNumbers.size()
        );

        return customerRepository.findByDocumentNumberInAndStatus(uniqueDocumentNumbers, true)
                .map(customer -> new CustomerSummaryResponse(
                        customer.getId(),
                        customer.getDocumentNumber(),
                        customer.isStatus()
                ))
                .collectList()
                .flatMap(customers ->{

                    if (!(customers.size() == uniqueDocumentNumbers.size())){
                        return Mono.error(new CustomersParticipantsException(
                                "Some customers do not exist or are inactive"
                        ));
                    }
                    return Mono.just(customers);


                }).doOnSuccess(customers ->
                        log.info(
                                "Customers search by document number list completed successfully. foundCount={}",
                                customers.size()
                        )
                )
                .doOnError(CustomersParticipantsException.class, error ->
                        log.warn(
                                "Some customers were not found or inactive. requestedCount={}",
                                uniqueDocumentNumbers.size()
                        )
                )
                .doOnError(error ->logUnexpectedError(
                        "Unexpected error searching customers by document number list",
                        error
                ));
    }

    private void logUnexpectedError(String message, Throwable error) {

        if (isExpectedBusinessError(error)) {
            return;
        }

        log.error(message, error);
    }

    private boolean isExpectedBusinessError(Throwable error) {

        return error instanceof ResourceNotFoundException
                || error instanceof AlreadyCustomerExistsException
                || error instanceof CustomersParticipantsException;
    }


    private String maskDocument(String documentNumber) {

        String cleanDocumentNumber = documentNumber.trim();

        String lastDigits = cleanDocumentNumber.substring(
                cleanDocumentNumber.length() - 4
        );

        return "****" + lastDigits;
    }

}
